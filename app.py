from flask import Flask, request, jsonify
from flask_cors import CORS
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteApi, SYNCHRONOUS
from influxdb_client.client.query_api import QueryApi
import os
from dotenv import load_dotenv
import json
import cloudinary
import cloudinary.uploader
import base64
from io import BytesIO
from datetime import datetime
import dateutil.parser
from zoneinfo import ZoneInfo
import traceback
import requests
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__, static_folder='static', static_url_path='/static')

# Enable CORS for all routes (allows browser-based pings from any origin)
CORS(app)

# Load environment variables
load_dotenv()

# InfluxDB configuration
INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'https://us-east-1-1.aws.cloud2.influxdata.com')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN', 'nZ49M1MTGbHtRCrc2OJhx-kVIBWuwvereT-o1mcq2COz3urUNuUuIIMjysObK8oOEHn8352w7LKFyrX8PQpdsA==')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', 'Agri')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', 'smart_agri')

# Cloudinary configuration
cloudinary.config(
    cloud_name=os.getenv('CLOUDINARY_CLOUD_NAME', 'dnjlsegrq'),
    api_key=os.getenv('CLOUDINARY_API_KEY', '315166364872797'),
    api_secret=os.getenv('CLOUDINARY_API_SECRET', 'xIrcgfB7euQCW-FKi0kd6nWur24'),
    secure=True
)
CLOUDINARY_UPLOAD_PRESET = os.getenv('CLOUDINARY_UPLOAD_PRESET', 'smart_agri_preset')

# Initialize InfluxDB client
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)
query_api = influx_client.query_api()

# Get the Render app URL from environment variable or fallback to known URL
RENDER_APP_URL = os.getenv('RENDER_EXTERNAL_URL', 'https://vimal-farm.onrender.com')

# APScheduler setup for keep-alive pings
def ping_self():
    """Send a GET request to the /healthz endpoint to keep the Render instance alive"""
    try:
        response = requests.get(f"{RENDER_APP_URL}/healthz", timeout=5)
        if response.status_code == 200:
            print(f"Self-ping successful at {datetime.now().isoformat()}: {response.json()}")
        else:
            print(f"Self-ping failed at {datetime.now().isoformat()}: Status {response.status_code}")
    except Exception as e:
        print(f"Self-ping error at {datetime.now().isoformat()}: {str(e)}")

scheduler = BackgroundScheduler()
scheduler.add_job(ping_self, 'interval', minutes=5)  # Ping every 5 minutes
scheduler.start()

# Expected English questions for validation (backend stores in English)
EXPECTED_QUESTIONS = {
    'Day 1 - Watering & Health': [
        'Did you water the plants today?',
        'Did it rain today on your field?',
        'Did you spray pesticide or fungicide?',
        'Did you remove weeds today?',
        'Is the plant healthy today (your view)?',
        'Any unusual weather (wind, hail, storm, excess heat)?'
    ],
    'Day 2 - Nutrients & Operations': [
        'Did you apply fertilizer today?',
        'Did you notice any pests or disease symptoms?',
        'Are the leaves showing any issues (spots, yellowing, curling)?',
        'Did you or any labor work in the field today?',
        'Did you face any irrigation or electricity issues?',
        'Did you complete the planned task for today?',
        'Any other field observation or issue today?'
    ],
    'Weekly Review': [
        'What stage is the crop in now?',
        'Is the crop growing as expected?',
        'Is your expected harvest yield still realistic?',
        'Have you planned for harvest storage or sale?',
        'Did any crop support (fence, net, stakes) need fixing this week?',
        'Did you consult anyone for crop advice?',
        'Do you want expert help or callback from our team?',
        'Any wildlife/cattle/animal damage this week?'
    ]
}

# Custom 404 handler that returns JSON
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.route('/')
def serve_index():
    return app.send_static_file('index.html')

@app.route('/static/<path:filename>')
def serve_static(filename):
    return app.send_static_file(filename)

@app.route('/ping', methods=['GET'])
def ping():
    """Keep-alive endpoint for Render - returns simple status to prevent spin-down"""
    print(f"Ping received at {datetime.now().isoformat()} - Instance is alive!")
    return jsonify({
        'status': 'alive', 
        'timestamp': datetime.now().isoformat(),
        'message': 'Farm Tracker API is running'
    }), 200

@app.route('/healthz', methods=['GET'])
def healthz():
    """Health check endpoint for external monitoring services and self-pinging"""
    print(f"Health check received at {datetime.now().isoformat()}")
    return jsonify({'status': 'healthy'}), 200

@app.route('/upload_image', methods=['POST'])
def upload_image():
    try:
        data = request.json
        image_data = data.get('image')
        question_id = data.get('question_id')
        timestamp = data.get('timestamp')

        if not image_data or not question_id:
            return jsonify({'error': 'Missing image or question_id'}), 400

        # Extract base64 data (remove "data:image/jpeg;base64," prefix)
        if ',' in image_data:
            image_data = image_data.split(',')[1]
        else:
            return jsonify({'error': 'Invalid base64 image data'}), 400

        # Create a safe public_id
        safe_timestamp = timestamp.replace(':', '-').replace('.', '-')
        public_id = f"smart_agri/{question_id}_{safe_timestamp}"

        # Upload to Cloudinary
        try:
            result = cloudinary.uploader.upload(
                f"data:image/jpeg;base64,{image_data}",
                upload_preset=CLOUDINARY_UPLOAD_PRESET,
                public_id=public_id,
                folder="smart_agri"
            )
            image_url = result['secure_url']
            print(f"Image uploaded successfully: {image_url}")
        except Exception as e:
            print(f"Cloudinary upload error: {str(e)}")
            return jsonify({'error': f'Failed to upload to Cloudinary: {str(e)}'}), 500

        return jsonify({'image_url': image_url}), 200
    except Exception as e:
        print(f"Server error in upload_image: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/save_responses', methods=['POST'])
def save_responses():
    try:
        server_date = datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%Y-%m-%d')
        print(f"Server date (IST): {server_date}")

        data = request.json
        date = data.get('date')
        question_type = data.get('type')
        language = data.get('language', 'hindi')
        responses = data.get('responses')
        crop_health_score = data.get('crop_health_score')
        timestamp = data.get('timestamp')

        if not responses:
            return jsonify({'error': 'No responses provided'}), 400
        if not question_type:
            print(f"Error: question_type is None or missing")
            return jsonify({'error': 'question_type is missing or invalid'}), 400
        if crop_health_score is None:
            print(f"Error: crop_health_score is missing")
            return jsonify({'error': 'crop_health_score is missing'}), 400

        # Validate questions against expected English questions
        received_questions = list(responses.keys())
        expected_questions = EXPECTED_QUESTIONS.get(question_type, [])
        if len(received_questions) > len(expected_questions):
            received_questions = received_questions[:len(expected_questions)]
        
        missing_questions = [q for q in received_questions if q not in expected_questions]
        if missing_questions:
            print(f"Warning: Some received questions don't match expected: {missing_questions}")

        print(f"Received responses: date={date}, type={question_type}, language={language}, timestamp={timestamp}, crop_health_score={crop_health_score}")
        print(f"Number of responses: {len(responses)}")

        # Convert ISO 8601 timestamp to Unix epoch nanoseconds
        try:
            parsed_time = dateutil.parser.isoparse(timestamp)
            timestamp_ns = int(parsed_time.timestamp() * 1_000_000_000)
        except ValueError as e:
            print(f"Invalid timestamp format: {timestamp}, error: {str(e)}")
            return jsonify({'error': f'Invalid timestamp format: {timestamp}'}), 400

        # Convert responses to InfluxDB Line Protocol
        lines = []
        valid_responses = 0
        
        for index, (question, response) in enumerate(responses.items()):
            if question not in expected_questions:
                print(f"Skipping invalid question: {question}")
                continue
                
            answer = str(response.get('answer', ''))
            followup_text = str(response.get('followupText', ''))
            photos_list = response.get('photos', [])
            
            if not answer and not followup_text and not photos_list:
                print(f"Skipping question {index + 1}: No meaningful data")
                continue

            valid_responses += 1
            
            # Escape special characters for InfluxDB Line Protocol
            def escape_field(value):
                if isinstance(value, str):
                    return value.replace('\\', '\\\\').replace('"', '\\"').replace(',', '\\,').replace(' ', '\\ ').replace('=', '\\=')
                return str(value)
            
            def escape_tag(value):
                if isinstance(value, str):
                    return value.replace('\\', '\\\\').replace(' ', '\\ ').replace(',', '\\,').replace('=', '\\=')
                return str(value)
            
            escaped_question = escape_field(question)
            escaped_answer = escape_field(answer)
            escaped_followup_text = escape_field(followup_text)
            
            photos_urls = [photo.get('url', '') for photo in photos_list if photo.get('url')]
            escaped_photos = json.dumps([{'url': url} for url in photos_urls]) if photos_urls else '[]'
            escaped_photos = escape_field(escaped_photos)

            # Build fields (all strings for consistency, except crop_health_score)
            fields = []
            if escaped_answer:
                fields.append(f'answer="{escaped_answer}"')
            if escaped_followup_text:
                fields.append(f'followup_text="{escaped_followup_text}"')
            if photos_urls:
                fields.append(f'photos="{escaped_photos}"')
            fields.append(f'question="{escaped_question}"')
            if index == 0:  # Add crop_health_score to the first record
                fields.append(f'crop_health_score={crop_health_score}')

            # Build tags (simplify type tag to reduce cardinality)
            tag_parts = []
            tag_parts.append(f"date={escape_tag(date)}")
            tag_parts.append(f"type={escape_tag(question_type.replace(' ', '_').replace('&', '_'))}")
            tag_parts.append(f"language={escape_tag(language)}")
            tag_parts.append(f"question_id=q{index + 1}")

            # Construct Line Protocol
            line = f'Vimal_Task,{",".join(tag_parts)} {",".join(fields)} {timestamp_ns}'
            lines.append(line)
            print(f"Generated line for q{index + 1}: {line}")

        if not lines:
            print("No valid data points to write to InfluxDB")
            return jsonify({'error': 'No valid responses to save'}), 400

        print(f"Prepared {len(lines)} valid data points for InfluxDB")

        # Write to InfluxDB and verify
        try:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=lines)
            print(f"Successfully wrote {len(lines)} records to InfluxDB bucket '{INFLUXDB_BUCKET}'")

            # Verify write
            query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: -1m)
                |> filter(fn: (r) => r["_measurement"] == "Vimal_Task")
                |> filter(fn: (r) => r["date"] == "{date}")
                |> limit(n: {len(lines)})
            '''
            tables = query_api.query(query=query, org=INFLUXDB_ORG)
            if not tables:
                print("Verification failed: No records found after write")
                # Check for rejections
                rejection_query = f'''
                from(bucket: "_monitoring")
                    |> range(start: -1h)
                    |> filter(fn: (r) => r["_measurement"] == "rejected_points")
                    |> filter(fn: (r) => r["bucket"] == "{INFLUXDB_BUCKET}")
                    |> limit(n: 10)
                '''
                rejections = query_api.query(query=rejection_query, org=INFLUXDB_ORG)
                if rejections:
                    errors = [record["_value"] for table in rejections for record in table.records]
                    print(f"Rejections found: {errors}")
                    return jsonify({'error': f'Write rejected: {errors}'}), 500
                return jsonify({'error': 'Write succeeded but data not found'}), 500

            print(f"Verified {len(tables)} tables written")
            return jsonify({
                'message': f'Responses saved successfully ({len(lines)} records)',
                'records_written': len(lines)
            }), 200
        except Exception as e:
            print(f"InfluxDB write error: {str(e)}")
            traceback.print_exc()
            return jsonify({'error': f'Failed to write to InfluxDB: {str(e)}'}), 500
            
    except Exception as e:
        print(f"Server error in save_responses: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/check_rejections', methods=['GET'])
def check_rejections():
    try:
        query = f'''
        from(bucket: "_monitoring")
            |> range(start: -24h)
            |> filter(fn: (r) => r["_measurement"] == "rejected_points")
            |> filter(fn: (r) => r["bucket"] == "{INFLUXDB_BUCKET}")
            |> limit(n: 100)
        '''
        tables = query_api.query(query=query, org=INFLUXDB_ORG)
        rejections = [
            {
                'time': record['_time'],
                'error': record['_value'],
                'line': record.get('line', 'N/A')
            }
            for table in tables for record in table.records
        ]
        return jsonify({'rejections': rejections}), 200
    except Exception as e:
        print(f"Error checking rejections: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': f'Failed to check rejections: {str(e)}'}), 500

if __name__ == '__main__':
    print("Starting Farm Tracker API...")
    print(f"Serving static files from: {os.path.abspath('static')}")
    port = int(os.environ.get('PORT', 5000))
    try:
        app.run(host='0.0.0.0', port=port, debug=False)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()  # Gracefully shutdown scheduler on app termination