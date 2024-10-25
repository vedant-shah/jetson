import json
import time
import mysql.connector
from threading import Timer, Lock
from copy import deepcopy

# Load frames from JSON
with open('dummy_video_frames.json', 'r') as file:
    frames = json.load(file)

# Track ID dictionary
track_dict = {}
track_dict_lock = Lock()

# Database connection setup (adjust as per your configuration)
db_config = {
    'user': 'root',
    'password': 'gscaiml',
    'host': 'localhost',
    'database': 'jetson'
}

# Initialize the connection
db_connection = mysql.connector.connect(**db_config)
db_cursor = db_connection.cursor()

# Ensure a table exists for storing track info
db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS track_data (
        track_id INT,
        Confidence FLOAT,
        ClassID INT,
        `Left` FLOAT,
        `Top` FLOAT,
        `Right` FLOAT,
        `Bottom` FLOAT,
        Width FLOAT,
        Height FLOAT,
        Area FLOAT,
        CenterX FLOAT,
        CenterY FLOAT,
        Time DATETIME
    )
''')

db_connection.commit()

# Function to write dictionary to DB every 60 seconds and clear it
def write_to_db():
    global track_dict

    # Create a copy of the dictionary to avoid modifying during iteration
    with track_dict_lock:
        track_dict_copy = track_dict.copy()

    # Insert data into MySQL
    for track_id, frame_data in track_dict_copy.items():
        for obj in frame_data:
            try:
                db_cursor.execute('''
                    INSERT INTO track_data (track_id, Confidence, ClassID, `Left`, `Top`, `Right`, `Bottom`, 
                                            Width, Height, Area, CenterX, CenterY, Time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    track_id,
                    obj["Confidence"],
                    obj["ClassID"],
                    obj["Left"],
                    obj["Top"],
                    obj["Right"],
                    obj["Bottom"],
                    obj["Width"],
                    obj["Height"],
                    obj["Area"],
                    obj["Center"][0],  # CenterX
                    obj["Center"][1],  # CenterY
                    obj["Time"]
                ))
            except mysql.connector.Error as err:
                print(f"Error: {err}")

    # Commit transaction and clear the original dictionary
    db_connection.commit()
    track_dict.clear()
    
    Timer(30, write_to_db).start()
    
    
# Start the periodic database write function
write_to_db()

count = 0;
# Iterate frames at 25 fps
for frame_index, frame in enumerate(frames):
    
    # Process each object in the current frame
    for obj in frame:
        
        count = count + 1
        
        track_id = obj["trackID"]
        
        # Add the full object properties to track_dict for the given track_id
        if track_id in track_dict:
            track_dict[track_id].append(obj)
        else:
            track_dict[track_id] = [obj]

    # Sleep to mimic real-time processing (25 frames per second)
    if (frame_index + 1) % 25 == 0:
        time.sleep(1)
print(count)

write_to_db()
# Close the DB connection after processing
db_cursor.close()
db_connection.close()
