import os
import sys
import subprocess
import uuid
import shutil
from pathlib import Path
import traceback # For detailed error logging

from flask import Flask, request, jsonify
from dotenv import load_dotenv
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.services.storage import Storage
from appwrite.input_file import InputFile
from appwrite.id import ID
from appwrite.exception import AppwriteException

load_dotenv()

FLASK_HOST = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
FLASK_PORT = int(os.getenv('FLASK_RUN_PORT', 49200))
ENDPOINT_SECRET = os.getenv('ENDPOINT_SECRET')
APPWRITE_ENDPOINT = os.getenv('APPWRITE_ENDPOINT')
APPWRITE_PROJECT_ID = os.getenv('APPWRITE_PROJECT_ID')
APPWRITE_API_KEY = os.getenv('APPWRITE_API_KEY')
APPWRITE_DATABASE_ID = os.getenv('APPWRITE_DATABASE_ID')
APPWRITE_JOBS_COLLECTION_ID = os.getenv('APPWRITE_JOBS_COLLECTION_ID')

APPWRITE_SOURCE_BUCKET_ID = os.getenv('APPWRITE_SOURCE_BUCKET_ID')
APPWRITE_TARGET_BUCKET_ID = os.getenv('APPWRITE_TARGET_BUCKET_ID')
APPWRITE_RESULT_BUCKET_ID = os.getenv('APPWRITE_RESULT_BUCKET_ID')

if not ENDPOINT_SECRET:
    print("Error: ENDPOINT_SECRET not set in .env file.", file=sys.stderr)
    sys.exit(1)

if not all([APPWRITE_ENDPOINT, APPWRITE_PROJECT_ID, APPWRITE_API_KEY, APPWRITE_DATABASE_ID, APPWRITE_JOBS_COLLECTION_ID]):
    print("Error: Missing required core Appwrite configuration in .env file.", file=sys.stderr)
    sys.exit(1)

if not all([APPWRITE_SOURCE_BUCKET_ID, APPWRITE_TARGET_BUCKET_ID, APPWRITE_RESULT_BUCKET_ID]):
    print("Error: Missing required Appwrite Storage bucket IDs (SOURCE, TARGET, RESULT) in .env file.", file=sys.stderr)
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent
UPLOADS_DIR = BASE_DIR / "uploads"
SOURCE_DIR = UPLOADS_DIR / "source"
TARGET_DIR = UPLOADS_DIR / "target"
OUTPUT_DIR = UPLOADS_DIR / "output"

SOURCE_DIR.mkdir(parents=True, exist_ok=True)
TARGET_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FACEFUSION_SCRIPT_PATH = BASE_DIR / "facefusion.py"

app = Flask(__name__)

client = Client()
client.set_endpoint(APPWRITE_ENDPOINT)
client.set_project(APPWRITE_PROJECT_ID)
client.set_key(APPWRITE_API_KEY)

databases = Databases(client)
storage = Storage(client)

def update_job_status(job_id, status, result_id=None):
    try:
        data = {'status': status}
        if result_id:
            data['resultId'] = result_id

        databases.update_document(
            database_id=APPWRITE_DATABASE_ID,
            collection_id=APPWRITE_JOBS_COLLECTION_ID,
            document_id=job_id,
            data=data
        )
        print(f"Job {job_id} status updated to {status}")
    except AppwriteException as e:
        print(f"Error updating job {job_id} status to {status}: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response:
             print(f"Appwrite Response (Update Error): {e.response}", file=sys.stderr)
    except Exception as e:
        print(f"Unexpected error updating job {job_id}: {e}", file=sys.stderr)

def get_file_extension(filename):
    if not filename or '.' not in filename:
        return ""
    return os.path.splitext(filename)[1].lower()

def is_video_file(file_path):
    """Check if the file is a video file based on extension"""
    video_extensions = ['.mp4', '.mov']
    return get_file_extension(str(file_path)) in video_extensions

def create_gif_preview(video_path, output_path, fps=10, width=320):
    """Create a GIF preview from a video file using ffmpeg"""
    command = [
        'ffmpeg',
        '-i', str(video_path),
        '-vf', f'fps={fps},scale={width}:-1:flags=lanczos',
        '-y',
        str(output_path)
    ]
    
    process = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        encoding='utf-8'
    )
    return process.returncode == 0

@app.route('/v1/swap-faces', methods=['POST'])
def swap_faces_endpoint():
    if not FACEFUSION_SCRIPT_PATH.is_file():
        print(f"Error: Facefusion script not found at {FACEFUSION_SCRIPT_PATH}", file=sys.stderr)
        return jsonify({"error": "Server configuration error: Processing script missing."}), 500

    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to parse JSON payload: {e}"}), 400

    req_secret = data.get('secret')
    if not req_secret or req_secret != ENDPOINT_SECRET:
        print("Unauthorized access attempt: Invalid secret", file=sys.stderr)
        return jsonify({"error": "Unauthorized"}), 401

    job_id = data.get('jobId')
    if not job_id:
        return jsonify({"error": "Missing 'jobId' parameter"}), 400

    print(f"\n--- Received job request: {job_id} ---")

    face_path = None
    media_path = None
    output_path = None

    try:
        print(f"Fetching job document for {job_id}...")
        job_doc = databases.get_document(
            database_id=APPWRITE_DATABASE_ID,
            collection_id=APPWRITE_JOBS_COLLECTION_ID,
            document_id=job_id
        )
        face_id = job_doc.get('faceId')
        media_id = job_doc.get('mediaId')

        if not face_id or not media_id:
            error_msg = "Missing faceId or mediaId in job document"
            print(f"Error for job {job_id}: {error_msg}", file=sys.stderr)
            update_job_status(job_id, 'failed')
            return jsonify({"error": error_msg}), 400

        print(f"  Job document found. Source ID: {face_id}, Target ID: {media_id}")

    except AppwriteException as e:
        if e.code == 404:
            error_msg = f"Job document not found: {job_id}"
            print(error_msg, file=sys.stderr)
            return jsonify({"error": error_msg}), 404
        else:
            error_msg = f"Appwrite error fetching job {job_id}: {e}"
            print(error_msg, file=sys.stderr)
            if hasattr(e, 'response') and e.response:
                print(f"Appwrite Response (Fetch Error): {e.response}", file=sys.stderr)
            return jsonify({"error": "Failed to fetch job details.", "details": str(e)}), 500
    except Exception as e:
        error_msg = f"Unexpected error fetching job {job_id}: {e}"
        print(error_msg, file=sys.stderr)
        traceback.print_exc()
        return jsonify({"error": "Internal server error during job fetch."}), 500

    try:
        print("Fetching file metadata...")
        try:
            source_file_meta = storage.get_file(APPWRITE_SOURCE_BUCKET_ID, face_id)
            target_file_meta = storage.get_file(APPWRITE_TARGET_BUCKET_ID, media_id)
            source_filename_original = source_file_meta['name']
            target_filename_original = target_file_meta['name']
            print(f"  Source original name: {source_filename_original}")
            print(f"  Target original name: {target_filename_original}")
        except AppwriteException as e:
            error_msg = f"Failed to get file metadata from Appwrite Storage. Error: {e}"
            print(error_msg, file=sys.stderr)
            if hasattr(e, 'response') and e.response:
                print(f"Appwrite Response (Metadata Error): {e.response}", file=sys.stderr)
            update_job_status(job_id, 'failed')
            return jsonify({"error": "Failed to get media file details.", "details": str(e)}), 500

        update_job_status(job_id, 'processing')

        print("Downloading media files...")
        source_ext = get_file_extension(source_filename_original)
        target_ext = get_file_extension(target_filename_original)

        source_filename_local = f"{job_id}_source_{uuid.uuid4().hex}{source_ext}"
        target_filename_local = f"{job_id}_target_{uuid.uuid4().hex}{target_ext}"
        face_path = SOURCE_DIR / source_filename_local
        media_path = TARGET_DIR / target_filename_local

        try:
            print(f"  Downloading Source (Bucket: {APPWRITE_SOURCE_BUCKET_ID}, File: {face_id})")
            source_bytes = storage.get_file_download(APPWRITE_SOURCE_BUCKET_ID, face_id)
            with open(face_path, 'wb') as f: f.write(source_bytes)
            print(f"    Downloaded source to: {face_path}")

            print(f"  Downloading Target (Bucket: {APPWRITE_TARGET_BUCKET_ID}, File: {media_id})")
            target_bytes = storage.get_file_download(APPWRITE_TARGET_BUCKET_ID, media_id)
            with open(media_path, 'wb') as f: f.write(target_bytes)
            print(f"    Downloaded target to: {media_path}")

        except AppwriteException as e:
            error_msg = f"Failed to download files from Appwrite Storage. Error: {e}"
            print(error_msg, file=sys.stderr)
            if hasattr(e, 'response') and e.response:
                print(f"Appwrite Response (Download Error): {e.response}", file=sys.stderr)
            update_job_status(job_id, 'failed')
            return jsonify({"error": "Failed to download media files.", "details": str(e)}), 500
        except Exception as e:
             error_msg = f"Failed write downloaded files locally: {e}"
             print(error_msg, file=sys.stderr)
             update_job_status(job_id, 'failed')
             return jsonify({"error": "Failed to save media files locally.", "details": str(e)}), 500

        output_filename = f"result_{job_id}_{uuid.uuid4().hex}{target_ext}"
        output_path = OUTPUT_DIR / output_filename

        command = [
            sys.executable,
            str(FACEFUSION_SCRIPT_PATH),
            'headless-run',
            '-s', str(face_path),
            '-t', str(media_path),
            '-o', str(output_path)
            # Add any other necessary facefusion args here
        ]

        print(f"Executing command: {' '.join(command)}")
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            encoding='utf-8'
        )

        # --- Updated Handle Command Result ---
        # Success check: return code 0 AND output file exists
        if process.returncode == 0 and output_path.exists():
            print("Face swapping process completed successfully (Return Code 0, Output File Exists).")
            print(f"  stdout:\n{process.stdout}")

            print(f"Uploading result file {output_path} to Appwrite Storage (Bucket: {APPWRITE_RESULT_BUCKET_ID})...")
            try:
                input_file = InputFile.from_path(str(output_path))
                upload_response = storage.create_file(
                    bucket_id=APPWRITE_RESULT_BUCKET_ID,
                    file_id=ID.unique(),
                    file=input_file,
                )
                result_id = upload_response['$id']
                print(f"  Upload successful. Result ID: {result_id}")
                
                # Check if the result is a video and create a GIF preview
                preview_id = None
                if is_video_file(output_path) and shutil.which('ffmpeg'):
                    print("Video result detected. Creating GIF preview...")
                    preview_path = OUTPUT_DIR / f"preview_{job_id}_{uuid.uuid4().hex}.gif"
                    if create_gif_preview(output_path, preview_path):
                        try:
                            preview_input_file = InputFile.from_path(str(preview_path))
                            preview_upload = storage.create_file(
                                bucket_id=APPWRITE_RESULT_BUCKET_ID,
                                file_id=ID.unique(),
                                file=preview_input_file,
                            )
                            preview_id = preview_upload['$id']
                            print(f"  GIF preview upload successful. Preview ID: {preview_id}")
                            
                            # Add the preview path to cleanup list
                            files_to_delete.append(preview_path)
                        except Exception as e:
                            print(f"Warning: Failed to upload GIF preview: {e}", file=sys.stderr)
                    else:
                        print("Warning: Failed to create GIF preview", file=sys.stderr)
                
                update_data = {'status': 'completed', 'resultId': result_id}
                if preview_id:
                    update_data['mediaPreviewId'] = preview_id
                
                # Update job with result ID and optional preview ID
                databases.update_document(
                    database_id=APPWRITE_DATABASE_ID,
                    collection_id=APPWRITE_JOBS_COLLECTION_ID,
                    document_id=job_id,
                    data=update_data
                )
                
                return jsonify({ "status": "success", "jobId": job_id, "resultId": result_id, "previewId": preview_id }), 200

            except AppwriteException as e:
                error_msg = f"Failed to upload result file to Appwrite Storage. Error: {e}"
                print(error_msg, file=sys.stderr)
                if hasattr(e, 'response') and e.response:
                    print(f"Appwrite Response (Upload Error): {e.response}", file=sys.stderr)
                update_job_status(job_id, 'failed')
                return jsonify({"error": "Failed to upload result file.", "details": str(e)}), 500
            except Exception as e:
                error_msg = f"Unexpected error during result upload: {e}"
                print(error_msg, file=sys.stderr)
                update_job_status(job_id, 'failed')
                return jsonify({"error": "Failed to upload result file.", "details": str(e)}), 500

        else:
            # Handle command failure based on return code or missing output file
            error_msg = f"Face swapping process failed."
            details = []
            if process.returncode != 0:
                details.append(f"Return code: {process.returncode}")
            if not output_path.exists():
                # This check is important if the process might return 0 but still fail to create the file
                details.append("Output file not created.")

            if details:
                 error_msg += f" ({'; '.join(details)})"
            else:
                # Should ideally not happen if the condition above failed, but as a fallback
                error_msg += " (Unknown reason)"

            print(error_msg, file=sys.stderr)

            stderr_output = process.stderr or "No stderr output."
            stdout_output = process.stdout or "No stdout output." # Log stdout even on failure
            print(f"  stderr:\n{stderr_output}", file=sys.stderr)
            print(f"  stdout:\n{stdout_output}", file=sys.stderr)

            update_job_status(job_id, 'failed')
            return jsonify({ "status": "error", "jobId": job_id, "message": error_msg, "details": stderr_output }), 500

    except Exception as e:
        error_msg = f"An unexpected error occurred during job {job_id} processing: {e}"
        print(error_msg, file=sys.stderr)
        traceback.print_exc()
        update_job_status(job_id, 'failed')
        return jsonify({"error": "An internal server error occurred during processing."}), 500

    finally:
        print(f"Cleaning up local files for job {job_id}...")
        files_to_delete = [face_path, media_path, output_path]
        for file_path in files_to_delete:
             if file_path and file_path.exists():
                try:
                    file_path.unlink()
                    print(f"  Deleted {file_path}")
                except OSError as e:
                     print(f"Warning: Error deleting file {file_path}: {e}", file=sys.stderr)
        print(f"--- Job {job_id} processing finished ---")

if __name__ == '__main__':
    print(f"Starting Flask server on {FLASK_HOST}:{FLASK_PORT}")
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False)
