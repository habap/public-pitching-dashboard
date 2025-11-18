"""
Pitching Data CSV Upload Application
Supports: Rapsodo, PitchLogic, and Trackman CSV imports
"""

import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
import json
from datetime import datetime
import re
import requests

# Page configuration
st.set_page_config(
    page_title="Pitching Data Upload",
    page_icon="⚾",
    layout="wide"
)

# Database connection configuration
# Store these in Streamlit secrets or environment variables in production
DB_CONFIG = {
    'host': st.secrets.get("DB_HOST", "localhost"),
    'database': st.secrets.get("DB_NAME", "pitching_dev"),
    'user': st.secrets.get("DB_USER", "root"),
    'password': st.secrets.get("DB_PASSWORD", ""),
    'port': st.secrets.get("DB_PORT", 3306)
}

# ============================================
# DATABASE FUNCTIONS
# ============================================

def get_db_connection():
    """Create database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        st.error(f"Error connecting to MySQL: {e}")
        return None

def get_my_ip():
    """Get the public IP address of this Streamlit Cloud instance"""
    try:
        response = requests.get('https://api.ipify.org?format=json', timeout=5)
        return response.json()['ip']
    except:
        return "Could not determine IP"

def get_players(conn):
    """Retrieve all active players"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT player_id, first_name, last_name,
               CONCAT(first_name, ' ', last_name) AS player_name,
               graduation_year, throws_hand, email,
               rapsodo_player_id, pitchlogic_player_id, trackman_player_id
        FROM players 
        WHERE is_active = TRUE
        ORDER BY last_name, first_name
    """)
    return cursor.fetchall()

def get_locations(conn):
    """Retrieve all locations"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT location_id, location_name
        FROM locations 
        ORDER BY location_name
    """)
    return cursor.fetchall()

def create_location(conn, location_name):
    """Create a new location and return its ID"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO locations (location_name)
        VALUES (%s)
    """, (location_name,))
    conn.commit()
    return cursor.lastrowid

def ensure_locations_table(conn):
    """Create locations table if it doesn't exist, or update existing table safely"""
    cursor = conn.cursor()
    
    # Create table if it doesn't exist (won't affect existing table)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            location_id INT AUTO_INCREMENT PRIMARY KEY,
            location_name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Try to add unique constraint if it doesn't exist
    # This will silently fail if constraint already exists
    try:
        cursor.execute("""
            ALTER TABLE locations 
            ADD UNIQUE INDEX idx_location_name_unique (location_name)
        """)
        conn.commit()
    except Exception as e:
        # Constraint might already exist, which is fine
        conn.rollback()
        pass
    
    # Try to add regular index for performance if it doesn't exist
    try:
        cursor.execute("""
            CREATE INDEX idx_location_name ON locations(location_name)
        """)
        conn.commit()
    except Exception as e:
        # Index might already exist, which is fine
        conn.rollback()
        pass

def match_player_name(pitcher_name, players_list, external_id=None, data_source=None):
    """Match pitcher name from CSV to player in database
    
    Tries various matching strategies:
    1. External system ID match (Rapsodo ID, PitchLogic ID, etc.) - PRIORITY
    2. Exact match (case-insensitive)
    3. Last name match
    4. First name + last name match
    5. Fuzzy matching (contains)
    
    Args:
        pitcher_name: Name from CSV
        players_list: List of player dicts from database
        external_id: Rapsodo/PitchLogic/Trackman player ID if available
        data_source: 'Rapsodo', 'PitchLogic', or 'Trackman'
    
    Returns:
        tuple: (player_id, match_info_dict) or (None, None)
    """
    if not pitcher_name or pd.isna(pitcher_name):
        return None, None
    
    # First priority: Match by external system ID
    if external_id and data_source:
        id_field = f"{data_source.lower()}_player_id"
        for player in players_list:
            if player.get(id_field) == str(external_id):
                return player['player_id'], {
                    'method': 'External ID',
                    'confidence': 'High',
                    'player': player
                }
    
    pitcher_name = str(pitcher_name).strip().lower()
    matches = []
    
    for player in players_list:
        full_name = f"{player['first_name']} {player['last_name']}".lower()
        first_name = player['first_name'].lower()
        last_name = player['last_name'].lower()
        
        # Exact match
        if pitcher_name == full_name:
            matches.append({
                'player_id': player['player_id'],
                'method': 'Exact Name',
                'confidence': 'High',
                'player': player
            })
            continue
        
        # Last name only match
        if pitcher_name == last_name:
            matches.append({
                'player_id': player['player_id'],
                'method': 'Last Name',
                'confidence': 'Medium',
                'player': player
            })
            continue
        
        # First name + Last name (reversed order)
        if pitcher_name == f"{last_name} {first_name}":
            matches.append({
                'player_id': player['player_id'],
                'method': 'Name (Reversed)',
                'confidence': 'High',
                'player': player
            })
            continue
        
        # Contains match (for partial names)
        if last_name in pitcher_name or pitcher_name in full_name:
            matches.append({
                'player_id': player['player_id'],
                'method': 'Partial Match',
                'confidence': 'Low',
                'player': player
            })
    
    if len(matches) == 0:
        return None, None
    elif len(matches) == 1:
        return matches[0]['player_id'], matches[0]
    else:
        # Multiple matches - return the highest confidence one
        # But flag that there are duplicates
        best_match = max(matches, key=lambda m: 
            {'High': 3, 'Medium': 2, 'Low': 1}[m['confidence']])
        best_match['has_duplicates'] = True
        best_match['all_matches'] = matches
        return best_match['player_id'], best_match

def extract_external_id(row, data_source):
    """Extract external player ID from CSV row if available"""
    if data_source == 'Rapsodo':
        # Common Rapsodo ID field names
        for field in ['PlayerId', 'Player ID', 'PlayerID', 'RapsodoID', 'Rapsodo ID']:
            if field in row and pd.notna(row[field]):
                return str(row[field])
    elif data_source == 'PitchLogic':
        for field in ['PlayerID', 'Player ID', 'PitchLogicID']:
            if field in row and pd.notna(row[field]):
                return str(row[field])
    elif data_source == 'Trackman':
        for field in ['PlayerID', 'Player ID', 'TrackmanID']:
            if field in row and pd.notna(row[field]):
                return str(row[field])
    return None

def format_player_display(player, show_ids=False):
    """Format player info for display with disambiguating details"""
    name = f"{player['first_name']} {player['last_name']}"
    details = []
    
    if player.get('graduation_year'):
        details.append(f"Class of {player['graduation_year']}")
    
    if player.get('throws_hand'):
        details.append(f"{player['throws_hand']}HP")
    
    if show_ids:
        if player.get('rapsodo_player_id'):
            details.append(f"Rapsodo: {player['rapsodo_player_id']}")
        if player.get('email'):
            details.append(f"Email: {player['email']}")
    
    if details:
        return f"{name} ({', '.join(details)})"
    return name

def create_player_from_name(conn, pitcher_name, external_id=None, data_source=None, pitcher_df=None):
    """Create a new player record from pitcher name and optional external ID
    
    Args:
        conn: Database connection
        pitcher_name: Full name string (e.g., "Silas Findley" or "John Paul Smith")
        external_id: Optional external system ID
        data_source: Optional data source name (Rapsodo, PitchLogic, Trackman)
        pitcher_df: Optional DataFrame with pitcher's data (used to determine handedness)
    
    Returns:
        int: New player_id
    """
    # Parse the name - handle various formats
    pitcher_name = str(pitcher_name).strip()
    names = pitcher_name.split()
    
    if len(names) == 0:
        first_name = "Unknown"
        last_name = "Player"
    elif len(names) == 1:
        first_name = names[0]
        last_name = "Unknown"
    elif len(names) == 2:
        first_name = names[0]
        last_name = names[1]
    else:
        # For names with 3+ parts (e.g., "John Paul Smith"), 
        # take first as first_name, rest as last_name
        first_name = names[0]
        last_name = ' '.join(names[1:])
    
    # Determine handedness from arm slot if available
    throws_hand = 'R'  # Default to right-handed
    
    if pitcher_df is not None and len(pitcher_df) > 0:
        # Try to find arm slot column (various naming conventions)
        arm_slot_col = None
        for col in ['Arm Slot (yellow)', 'Arm Slot', 'ArmSlot', 'arm_slot']:
            if col in pitcher_df.columns:
                arm_slot_col = col
                break
        
        if arm_slot_col:
            # Get arm slot values (use median to avoid outliers)
            arm_slot_values = pitcher_df[arm_slot_col].dropna()
            if len(arm_slot_values) > 0:
                # Try multiple pitches to get consistent reading
                handedness_votes = []
                for value in arm_slot_values.head(5):  # Check up to 5 pitches
                    determined_hand = determine_handedness_from_arm_slot(value)
                    if determined_hand:
                        handedness_votes.append(determined_hand)
                
                # Use majority vote if we have any determinations
                if handedness_votes:
                    right_count = handedness_votes.count('R')
                    left_count = handedness_votes.count('L')
                    if left_count > right_count:
                        throws_hand = 'L'
                    else:
                        throws_hand = 'R'
    
    cursor = conn.cursor()
    
    # Build query with optional external ID
    if external_id and data_source:
        id_field = f"{data_source.lower()}_player_id"
        query = f"""
            INSERT INTO players (first_name, last_name, throws_hand, is_active, {id_field})
            VALUES (%s, %s, %s, TRUE, %s)
        """
        cursor.execute(query, (first_name, last_name, throws_hand, external_id))
    else:
        query = """
            INSERT INTO players (first_name, last_name, throws_hand, is_active)
            VALUES (%s, %s, %s, TRUE)
        """
        cursor.execute(query, (first_name, last_name, throws_hand))
    
    conn.commit()
    player_id = cursor.lastrowid
    
    return player_id

def get_data_sources(conn):
    """Retrieve all data sources"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT source_id, source_name FROM data_sources ORDER BY source_name")
    return cursor.fetchall()

def create_training_session(conn, player_id, session_date, session_type, data_source_id, 
                           total_pitches, location="", coach_id=None, session_focus=""):
    """Create a new training session"""
    cursor = conn.cursor()
    query = """
        INSERT INTO training_sessions 
        (player_id, session_date, session_type, location, total_pitches, 
         coach_id, data_source_id, session_focus)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (player_id, session_date, session_type, location, 
                          total_pitches, coach_id, data_source_id, session_focus))
    conn.commit()
    return cursor.lastrowid

# ============================================
# PITCH TYPE STANDARDIZATION
# ============================================

PITCH_TYPE_MAP = {
    'FF': '4FB', 'FA': '4FB', '4-SEAM': '4FB', 'FASTBALL': '4FB', '4FB': '4FB', '4SEAM': '4FB',
    'FT': '2FB', 'SI': 'SI', '2-SEAM': '2FB', '2FB': '2FB', 'SINKER': 'SI', '2SEAM': '2FB',
    'FC': 'CT', 'CUTTER': 'CT', 'CT': 'CT',
    'CU': 'CB', 'CURVE': 'CB', 'CURVEBALL': 'CB', 'CB': 'CB',
    'SL': 'SL', 'SLIDER': 'SL',
    'CH': 'CH', 'CHANGEUP': 'CH', 'CHANGE': 'CH',
    'FS': 'SPL', 'SPLIT': 'SPL', 'SPLITTER': 'SPL', 'SPL': 'SPL',
    'KN': 'KN', 'KNUCKLEBALL': 'KN',
    'SB': 'SB', 'SCREWBALL': 'SB',
}

def standardize_pitch_type(pitch_type):
    """Standardize pitch type abbreviation"""
    if pd.isna(pitch_type) or pitch_type == '':
        return None
    pitch_type = str(pitch_type).strip().upper().replace(' ', '')
    return PITCH_TYPE_MAP.get(pitch_type, pitch_type)

# ============================================
# TILT CONVERSION (Clock to Degrees)
# ============================================

def tilt_to_degrees(tilt_str):
    """Convert clock tilt (e.g., '1:30' or '12:40') to degrees"""
    if pd.isna(tilt_str) or tilt_str == '':
        return None
    try:
        tilt_str = str(tilt_str).strip()
        
        # If it's already a number, return it
        try:
            return float(tilt_str)
        except:
            pass
        
        # Handle "1:30" format (clock time)
        if ':' in tilt_str:
            parts = tilt_str.split(':')
            hours = int(parts[0])
            minutes = int(parts[1])
            
            # Convert to degrees: each hour = 30°, each minute = 0.5°
            degrees = ((hours % 12) * 30 + (minutes / 2)) % 360
            return degrees
        
        # If no colon, try to parse as decimal
        return float(tilt_str)
    except Exception as e:
        # If all parsing fails, return None
        return None

def determine_handedness_from_arm_slot(arm_slot_value):
    """Determine pitcher handedness from arm slot angle.
    
    Arm slot is measured in degrees (0-360) or clock position.
    - Right-handed pitchers: typically 30-150 degrees (1:00-5:00)
    - Left-handed pitchers: typically 210-330 degrees (7:00-11:00)
    
    Args:
        arm_slot_value: Arm slot value (can be degrees, clock time, or None)
    
    Returns:
        str: 'R' for right-handed, 'L' for left-handed, None if cannot determine
    """
    if pd.isna(arm_slot_value):
        return None
    
    # Convert to degrees if needed
    arm_slot_degrees = tilt_to_degrees(arm_slot_value)
    
    if arm_slot_degrees is None:
        return None
    
    # Normalize to 0-360 range
    arm_slot_degrees = arm_slot_degrees % 360
    
    # Right-handed pitchers: release from right side (30-150 degrees, or 1:00-5:00)
    # Left-handed pitchers: release from left side (210-330 degrees, or 7:00-11:00)
    
    if 30 <= arm_slot_degrees <= 150:
        return 'R'
    elif 210 <= arm_slot_degrees <= 330:
        return 'L'
    else:
        # Ambiguous range (straight overhand or submarine)
        # Default to R as it's more common
        return None

# ============================================
# DATA SOURCE DETECTION
# ============================================

def detect_data_source(df):
    """Detect data source from CSV headers"""
    columns = [col.lower().strip() for col in df.columns]
    columns_joined = ' '.join(columns)
    
    # Rapsodo indicators
    rapsodo_indicators = ['relspeed', 'inducedvertbreak', 'tilt']
    if any(ind in columns_joined for ind in rapsodo_indicators):
        return 'Rapsodo'
    
    # PitchLogic indicators - handle various formats with/without spaces
    # Look for: "arm slot", "armslot", "gyro", "rifle spin"
    pitchlogic_indicators = ['arm slot', 'armslot', 'rifle spin', 'gyro']
    if any(ind in columns_joined for ind in pitchlogic_indicators):
        return 'PitchLogic'
    
    # Trackman indicators
    trackman_indicators = ['vertreangle', 'horzreangle', 'pitchcall', 'zonespeed']
    if any(ind in columns_joined for ind in trackman_indicators):
        return 'Trackman'
    
    return 'Unknown'

def detect_bulk_mode(df, data_source):
    """Detect if CSV contains multiple players"""
    if data_source == 'PitchLogic':
        # PitchLogic can have pitcher name in different formats
        if 'Pitcher Name' in df.columns or 'Pitcher' in df.columns:
            pitcher_col = 'Pitcher Name' if 'Pitcher Name' in df.columns else 'Pitcher'
            unique_pitchers = df[pitcher_col].nunique()
            return unique_pitchers > 1, unique_pitchers
        # Check for separate First Name / Last Name columns
        elif 'First Name' in df.columns and 'Last Name' in df.columns:
            # Create combined name and check uniqueness
            df['_temp_full_name'] = df['First Name'].astype(str) + ' ' + df['Last Name'].astype(str)
            unique_pitchers = df['_temp_full_name'].nunique()
            return unique_pitchers > 1, unique_pitchers
    
    elif data_source == 'Rapsodo':
        # Rapsodo often has pitcher in first few rows or in a 'Pitcher' column
        if 'Pitcher' in df.columns:
            unique_pitchers = df['Pitcher'].nunique()
            return unique_pitchers > 1, unique_pitchers
    
    return False, 1

# ============================================
# FIELD MAPPING FUNCTIONS
# ============================================

def map_rapsodo_fields(row):
    """Map Rapsodo CSV fields to database fields"""
    data = {}
    
    # Basic fields
    data['release_speed'] = row.get('Velocity') or row.get('RelSpeed')
    data['spin_rate'] = row.get('SpinRate')
    data['spin_axis'] = row.get('SpinAxis')
    
    # Handle Tilt conversion
    if 'Tilt' in row and pd.notna(row['Tilt']):
        data['spin_axis'] = tilt_to_degrees(row['Tilt'])
    
    data['spin_efficiency'] = row.get('SpinEff')
    data['horizontal_break'] = row.get('HorzBreak')
    data['induced_vertical_break'] = row.get('InducedVertBreak') or row.get('iVB')
    data['vertical_break'] = row.get('VertBreak')
    data['release_height'] = row.get('RelHeight')
    data['release_side'] = row.get('RelSide')
    data['release_extension'] = row.get('Extension')
    data['plate_location_x'] = row.get('PlateLocSide')
    data['plate_location_z'] = row.get('PlateLocHeight')
    
    # Rapsodo specific
    data['true_spin'] = row.get('TrueSpin')
    data['break_length'] = row.get('BreakLength')
    data['break_y'] = row.get('BreakY')
    data['spin_direction'] = row.get('SpinDirection')
    
    # Results
    data['exit_velocity'] = row.get('ExitSpeed')
    data['launch_angle'] = row.get('Angle')
    
    return data

def map_pitchlogic_fields(row):
    """Map PitchLogic CSV fields to database fields"""
    data = {}
    
    # Basic fields - handle various PitchLogic naming conventions
    data['release_speed'] = row.get('Velo') or row.get('Speed') or row.get('Speed (mph)')
    data['spin_rate'] = (row.get('Spin') or row.get('SpinRate') or row.get('Spin Rate') or 
                         row.get('Total Spin (rpm)'))
    
    # Spin axis - convert from clock time if needed
    spin_axis_raw = (row.get('Axis') or row.get('SpinAxis') or row.get('Spin Axis') or
                     row.get('Spin Direction (blue)'))
    data['spin_axis'] = tilt_to_degrees(spin_axis_raw) if spin_axis_raw else None
    
    # Movement fields
    data['horizontal_break'] = (row.get('HB') or row.get('Horizontal Break') or 
                                row.get('Horizontal Movement (in)'))
    data['vertical_break'] = (row.get('VB') or row.get('Vertical Break') or 
                              row.get('Vertical Movement (in)'))
    data['release_height'] = row.get('RH') or row.get('Release Height')
    data['release_side'] = row.get('RS') or row.get('Release Side')
    
    # PitchLogic specific fields
    # Arm slot - convert from clock time if needed
    arm_slot_raw = row.get('ArmSlot') or row.get('Arm Slot') or row.get('Arm Slot (yellow)')
    data['arm_slot'] = tilt_to_degrees(arm_slot_raw) if arm_slot_raw else None
    
    data['gyro_degree'] = row.get('Gyro') or row.get('Rifle Spin (rpm)')
    
    # Calculate relative spin direction if we have both values
    if pd.notna(data.get('spin_axis')) and pd.notna(data.get('arm_slot')):
        try:
            data['relative_spin_direction'] = abs(float(data['spin_axis']) - float(data['arm_slot']))
        except:
            pass
    
    # Spin efficiency
    data['spin_efficiency'] = row.get('SpinEff') or row.get('Spin Efficiency') or row.get('Spin Efficiency (%)')
    
    # Additional PitchLogic fields
    data['release_extension'] = (row.get('Forward Extension (ft)') or 
                                 row.get('Extension'))
    
    return data

def map_trackman_fields(row):
    """Map Trackman CSV fields to database fields"""
    data = {}
    
    # Basic fields
    data['release_speed'] = row.get('RelSpeed')
    data['spin_rate'] = row.get('SpinRate')
    data['spin_axis'] = row.get('SpinAxis')
    
    # Handle Tilt if present
    if 'Tilt' in row and pd.notna(row['Tilt']):
        data['spin_axis'] = tilt_to_degrees(row['Tilt'])
    
    data['horizontal_break'] = row.get('HorzBreak')
    data['induced_vertical_break'] = row.get('InducedVertBreak')
    data['vertical_break'] = row.get('VertBreak')
    data['release_height'] = row.get('RelHeight')
    data['release_side'] = row.get('RelSide')
    data['release_extension'] = row.get('Extension')
    data['plate_location_x'] = row.get('PlateLocSide')
    data['plate_location_z'] = row.get('PlateLocHeight')
    
    # Trackman specific - Release angles
    data['vert_rel_angle'] = row.get('VertRelAngle')
    data['horz_rel_angle'] = row.get('HorzRelAngle')
    
    # Trackman specific - Approach angles
    data['vert_approach_angle'] = row.get('VertApprAngle')
    data['horz_approach_angle'] = row.get('HorzApprAngle')
    
    # Trackman specific - Zone speed
    data['zone_speed'] = row.get('ZoneSpeed')
    
    # Trackman specific - PITCHf/x
    data['pfx_x'] = row.get('pfx_x')
    data['pfx_z'] = row.get('pfx_z')
    
    # Trackman specific - Contact data
    data['contact_position_x'] = row.get('ContactPositionX')
    data['contact_position_z'] = row.get('ContactPositionZ')
    data['hit_spin_rate'] = row.get('HitSpinRate')
    data['hang_time'] = row.get('HangTime')
    data['distance_feet'] = row.get('Distance')
    
    # Game context
    data['pitch_call'] = row.get('PitchCall')
    data['batter_side'] = row.get('BatterSide')
    data['balls'] = row.get('Balls')
    data['strikes'] = row.get('Strikes')
    data['outs'] = row.get('Outs')
    
    # Results
    data['exit_velocity'] = row.get('ExitSpeed')
    data['launch_angle'] = row.get('Angle') or row.get('LaunchAngle')
    
    return data

# ============================================
# DATA VALIDATION
# ============================================

def validate_pitch_data(pitch_dict):
    """Validate pitch data and return quality score"""
    score = 1.0
    issues = []
    
    # Check velocity
    if pd.notna(pitch_dict.get('release_speed')):
        velo = float(pitch_dict['release_speed'])
        if not (40 <= velo <= 110):
            score -= 0.3
            issues.append(f"Velocity {velo} mph outside normal range")
    
    # Check spin rate
    if pd.notna(pitch_dict.get('spin_rate')):
        spin = float(pitch_dict['spin_rate'])
        if not (500 <= spin <= 3500):
            score -= 0.3
            issues.append(f"Spin rate {spin} rpm outside normal range")
    
    # Check spin axis
    if pd.notna(pitch_dict.get('spin_axis')):
        axis = float(pitch_dict['spin_axis'])
        if not (0 <= axis <= 360):
            score -= 0.2
            issues.append(f"Spin axis {axis}° outside 0-360 range")
    
    # Check release height
    if pd.notna(pitch_dict.get('release_height')):
        height = float(pitch_dict['release_height'])
        if not (3 <= height <= 8):
            score -= 0.2
            issues.append(f"Release height {height} ft unusual")
    
    return max(0.0, score), issues

# ============================================
# CSV PROCESSING
# ============================================

def process_csv(df, player_id, data_source_name, session_id, filename, bulk_mode=False, 
                auto_create_players=False, players_list=None, bulk_location=None, bulk_session_type=None):
    """Process CSV and insert pitch data
    
    Args:
        df: DataFrame with pitch data
        player_id: Single player ID (used when bulk_mode=False)
        data_source_name: Name of data source (Rapsodo, PitchLogic, Trackman)
        session_id: Training session ID (used when bulk_mode=False)
        filename: Original filename
        bulk_mode: If True, extract player from each row
        auto_create_players: If True, create new players when not found
        players_list: List of players for matching (required for bulk_mode)
        bulk_location: Location for all sessions in bulk upload (optional)
        bulk_session_type: Session type for all sessions in bulk upload (optional, defaults to "Bullpen")
    """
    conn = get_db_connection()
    if not conn:
        return False, "Database connection failed", {}
    
    cursor = conn.cursor()
    
    # Get data source ID
    cursor.execute("SELECT source_id FROM data_sources WHERE source_name = %s", (data_source_name,))
    result = cursor.fetchone()
    if not result:
        return False, f"Data source '{data_source_name}' not found in database", {}
    data_source_id = result[0]
    
    # Track statistics
    stats = {
        'inserted': 0,
        'skipped': 0,
        'players_created': 0,
        'sessions_created': 0,
        'players_processed': {},
        'errors': []
    }
    
    # For bulk mode, group by player
    if bulk_mode:
        # Determine pitcher name column
        pitcher_col = None
        combine_names = False
        
        for col in ['Pitcher Name', 'Pitcher', 'pitcher', 'pitcher_name']:
            if col in df.columns:
                pitcher_col = col
                break
        
        # Check for separate First/Last name columns (PitchLogic format)
        if not pitcher_col and 'First Name' in df.columns and 'Last Name' in df.columns:
            df['_pitcher_full_name'] = df['First Name'].astype(str) + ' ' + df['Last Name'].astype(str)
            pitcher_col = '_pitcher_full_name'
            combine_names = True
        
        if not pitcher_col:
            return False, "Bulk mode enabled but no pitcher column found in CSV", stats
        
        # Group by pitcher
        grouped = df.groupby(pitcher_col)
        
        for pitcher_name, pitcher_df in grouped:
            # Try to extract external ID from first row
            first_row = pitcher_df.iloc[0]
            external_id = extract_external_id(first_row, data_source_name)
            
            # Match or create player
            matched_player_id, match_info = match_player_name(
                pitcher_name, players_list, external_id, data_source_name
            )
            
            # Handle duplicate name warnings
            if match_info and match_info.get('has_duplicates'):
                duplicate_warning = f"⚠️ Multiple matches found for '{pitcher_name}':"
                for match in match_info['all_matches']:
                    duplicate_warning += f"\n  - {format_player_display(match['player'], show_ids=True)}"
                duplicate_warning += f"\n  Using: {format_player_display(match_info['player'], show_ids=True)}"
                stats['errors'].append(duplicate_warning)
            
            if not matched_player_id and auto_create_players:
                try:
                    matched_player_id = create_player_from_name(
                        conn, pitcher_name, external_id, data_source_name, pitcher_df
                    )
                    stats['players_created'] += 1
                    id_info = f" (ID: {external_id})" if external_id else ""
                    
                    # Get the handedness that was determined
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT throws_hand FROM players WHERE player_id = %s", (matched_player_id,))
                    player_info = cursor.fetchone()
                    hand_display = "RHP" if player_info['throws_hand'] == 'R' else "LHP"
                    
                    st.info(f"✅ Created new player: {pitcher_name}{id_info} → Database ID: {matched_player_id} ({hand_display})")
                except Exception as e:
                    error_msg = f"❌ Failed to create player '{pitcher_name}': {str(e)}"
                    stats['errors'].append(error_msg)
                    st.error(error_msg)
                    matched_player_id = None
            elif not matched_player_id and not auto_create_players:
                # Player not found and auto-create is disabled
                skip_msg = f"⚠️ Skipped player '{pitcher_name}' - not found in database and auto-create disabled"
                stats['errors'].append(skip_msg)
            
            if not matched_player_id:
                stats['skipped'] += len(pitcher_df)
                stats['errors'].append(f"Could not match pitcher '{pitcher_name}' - {len(pitcher_df)} pitches skipped")
                continue
            
            # Group this player's data by date to create separate sessions
            # Extract dates for all pitches
            if 'Date' in pitcher_df.columns:
                pitcher_df['_session_date'] = pd.to_datetime(pitcher_df['Date']).dt.date
            else:
                # If no date column, use today for all
                pitcher_df['_session_date'] = datetime.now().date()
            
            # Group by date
            date_groups = pitcher_df.groupby('_session_date')
            
            for session_date, date_df in date_groups:
                # Create a separate session for each date
                session_type_to_use = bulk_session_type if bulk_session_type else "Bullpen"
                location_to_use = bulk_location if bulk_location else ""
                
                matched_session_id = create_training_session(
                    conn, matched_player_id, session_date, session_type_to_use, 
                    data_source_id, len(date_df), location_to_use, None, 
                    f"Bulk upload from {filename} - {session_date}"
                )
                stats['sessions_created'] += 1
                
                # Process pitches for this player on this date
                player_stats = process_pitcher_data(
                    conn, date_df, matched_player_id, data_source_name, 
                    data_source_id, matched_session_id, filename
                )
                
                stats['inserted'] += player_stats['inserted']
                stats['skipped'] += player_stats['skipped']
                stats['errors'].extend(player_stats['errors'])
            
            # Track per-player stats (summary across all dates)
            stats['players_processed'][pitcher_name] = {
                'player_id': matched_player_id,
                'pitches': len(pitcher_df),
                'sessions': len(date_groups),
                'external_id': external_id
            }
    
    else:
        # Single player mode - process all rows for one player
        player_stats = process_pitcher_data(
            conn, df, player_id, data_source_name, 
            data_source_id, session_id, filename
        )
        stats['inserted'] = player_stats['inserted']
        stats['skipped'] = player_stats['skipped']
        stats['errors'] = player_stats['errors']
    
    conn.close()
    
    # Generate summary message
    summary = f"✅ Successfully inserted {stats['inserted']} pitches"
    
    if bulk_mode:
        summary += f"\n👥 Processed {len(stats['players_processed'])} players"
        summary += f"\n📊 Created {stats['sessions_created']} sessions"
        if stats['players_created'] > 0:
            summary += f"\n➕ Created {stats['players_created']} new players"
    
    if stats['skipped'] > 0:
        summary += f"\n⚠️ Skipped {stats['skipped']} pitches"
    
    if stats['errors'] and len(stats['errors']) <= 10:
        summary += "\n\n⚠️ Issues:\n" + "\n".join(stats['errors'])
    elif stats['errors']:
        summary += f"\n\n⚠️ {len(stats['errors'])} issues encountered (showing first 10):\n"
        summary += "\n".join(stats['errors'][:10])
    
    return True, summary, stats

def extract_session_date(df):
    """Extract session date from dataframe"""
    if 'Date' in df.columns:
        date_val = df['Date'].iloc[0]
        try:
            return pd.to_datetime(date_val).date()
        except:
            pass
    return datetime.now().date()

def process_pitcher_data(conn, df, player_id, data_source_name, data_source_id, session_id, filename):
    """Process pitch data for a single pitcher"""
    cursor = conn.cursor()
    
    stats = {
        'inserted': 0,
        'skipped': 0,
        'errors': []
    }
    
    for idx, row in df.iterrows():
        try:
            # Map fields based on data source
            if data_source_name == 'Rapsodo':
                pitch_data = map_rapsodo_fields(row)
            elif data_source_name == 'PitchLogic':
                pitch_data = map_pitchlogic_fields(row)
            elif data_source_name == 'Trackman':
                pitch_data = map_trackman_fields(row)
            else:
                pitch_data = {}
            
            # Extract common fields - handle multiple naming conventions
            pitch_type = standardize_pitch_type(
                row.get('TaggedPitchType') or 
                row.get('Pitch Type') or 
                row.get('PitchType') or
                row.get('Type')  # PitchLogic uses just 'Type'
            )
            pitch_number = row.get('PitchNo') or row.get('Pitch #') or row.get('Pitch') or (idx + 1)
            
            # Handle date/time
            pitch_timestamp = None
            if 'Date' in row and pd.notna(row['Date']):
                date_str = str(row['Date'])
                time_str = str(row.get('Time', '00:00:00'))
                try:
                    pitch_timestamp = pd.to_datetime(f"{date_str} {time_str}")
                except:
                    pitch_timestamp = pd.to_datetime(date_str)
            
            # Validate data
            quality_score, issues = validate_pitch_data(pitch_data)
            
            # Store raw data as JSON - ensure it's valid
            raw_data = row.to_dict()
            # Remove any NaN or inf values that can't be serialized
            raw_data_clean = {}
            for k, v in raw_data.items():
                if pd.isna(v):
                    raw_data_clean[k] = None
                elif isinstance(v, (int, float)):
                    if pd.isna(v) or v == float('inf') or v == float('-inf'):
                        raw_data_clean[k] = None
                    else:
                        raw_data_clean[k] = float(v) if isinstance(v, float) else int(v)
                else:
                    raw_data_clean[k] = str(v)
            
            try:
                raw_json = json.dumps(raw_data_clean, default=str)
            except Exception as e:
                # If JSON serialization fails, store minimal data
                raw_json = json.dumps({'error': 'Could not serialize', 'row': idx}, default=str)
            
            # Build insert query
            query = """
                INSERT INTO pitch_data (
                    player_id, session_id, data_source_id, pitch_timestamp, pitch_number,
                    pitch_type, release_speed, spin_rate, spin_axis, spin_efficiency,
                    horizontal_break, vertical_break, induced_vertical_break,
                    release_height, release_side, release_extension,
                    plate_location_x, plate_location_z,
                    exit_velocity, launch_angle,
                    true_spin, spin_direction, break_length, break_y,
                    arm_slot, relative_spin_direction, gyro_degree,
                    vert_rel_angle, horz_rel_angle, vert_approach_angle, horz_approach_angle,
                    zone_speed, pfx_x, pfx_z,
                    contact_position_x, contact_position_z, hit_spin_rate, hang_time, distance_feet,
                    pitch_call, batter_side, balls, strikes, outs,
                    data_quality_score, raw_data_json, original_filename, is_valid_pitch
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            
            values = (
                player_id, session_id, data_source_id, pitch_timestamp, pitch_number,
                pitch_type, 
                pitch_data.get('release_speed'), 
                pitch_data.get('spin_rate'), 
                pitch_data.get('spin_axis'), 
                pitch_data.get('spin_efficiency'),
                pitch_data.get('horizontal_break'), 
                pitch_data.get('vertical_break'), 
                pitch_data.get('induced_vertical_break'),
                pitch_data.get('release_height'), 
                pitch_data.get('release_side'), 
                pitch_data.get('release_extension'),
                pitch_data.get('plate_location_x'), 
                pitch_data.get('plate_location_z'),
                pitch_data.get('exit_velocity'), 
                pitch_data.get('launch_angle'),
                pitch_data.get('true_spin'), 
                pitch_data.get('spin_direction'), 
                pitch_data.get('break_length'), 
                pitch_data.get('break_y'),
                pitch_data.get('arm_slot'), 
                pitch_data.get('relative_spin_direction'), 
                pitch_data.get('gyro_degree'),
                pitch_data.get('vert_rel_angle'), 
                pitch_data.get('horz_rel_angle'), 
                pitch_data.get('vert_approach_angle'), 
                pitch_data.get('horz_approach_angle'),
                pitch_data.get('zone_speed'), 
                pitch_data.get('pfx_x'), 
                pitch_data.get('pfx_z'),
                pitch_data.get('contact_position_x'), 
                pitch_data.get('contact_position_z'), 
                pitch_data.get('hit_spin_rate'), 
                pitch_data.get('hang_time'), 
                pitch_data.get('distance_feet'),
                pitch_data.get('pitch_call'), 
                pitch_data.get('batter_side'), 
                pitch_data.get('balls'), 
                pitch_data.get('strikes'), 
                pitch_data.get('outs'),
                quality_score, raw_json, filename, quality_score >= 0.5
            )
            
            cursor.execute(query, values)
            stats['inserted'] += 1
            
        except Exception as e:
            stats['skipped'] += 1
            error_msg = f"Row {idx + 1}: {str(e)}"
            if len(stats['errors']) < 10:  # Limit stored errors
                stats['errors'].append(error_msg)
    
    conn.commit()
    return stats

# ============================================
# STREAMLIT UI
# ============================================

def main():
    st.title("⚾ Pitching Data CSV Upload")
    st.markdown("Upload CSV files from Rapsodo, PitchLogic, or Trackman")
    
    # Initialize session state
    if 'uploaded_file' not in st.session_state:
        st.session_state.uploaded_file = None
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'detected_source' not in st.session_state:
        st.session_state.detected_source = None
    if 'upload_mode_selected' not in st.session_state:
        st.session_state.upload_mode_selected = False
    if 'bulk_mode' not in st.session_state:
        st.session_state.bulk_mode = False
    if 'upload_complete' not in st.session_state:
        st.session_state.upload_complete = False
    if 'upload_summary' not in st.session_state:
        st.session_state.upload_summary = None
    
    # Sidebar - Database connection status
    with st.sidebar:
        st.header("Database Connection")

        # Show what IP this app is running from
        my_ip = get_my_ip()
        st.info(f"🌐 This app's IP: {my_ip}")

        conn = get_db_connection()
        if conn:
            st.success("✅ Connected to database")
            # Ensure locations table exists
            ensure_locations_table(conn)
            conn.close()
        else:
            st.error("❌ Database connection failed")
            st.info("Check your database configuration in secrets.toml")
            return
        
        st.markdown("---")
        st.header("Upload Guide")
        st.markdown("""
        **Supported Formats:**
        - Rapsodo CSV exports
        - PitchLogic CSV exports  
        - Trackman CSV exports
        
        **Upload Modes:**
        - **Single Player:** All data for one player
        - **Bulk Upload:** Multiple players in one file
        
        **Required Fields:**
        - Player identification (bulk mode)
        - Date
        - At least velocity OR spin rate
        """)
    
    # Main content
    conn = get_db_connection()
    players = get_players(conn)
    
    # STEP 1: Upload CSV File (always shown unless upload is complete)
    if not st.session_state.upload_complete:
        st.header("1️⃣ Upload CSV File")
        uploaded_file = st.file_uploader("Choose a CSV file", type=['csv'], key='file_uploader')
        
        if uploaded_file is not None:
            # Only process if it's a new file
            if st.session_state.uploaded_file != uploaded_file.name:
                st.session_state.uploaded_file = uploaded_file.name
                st.session_state.upload_mode_selected = False
                
                # Read CSV
                try:
                    st.session_state.df = pd.read_csv(uploaded_file)
                    df = st.session_state.df
                    st.success(f"✅ File loaded: {len(df)} rows, {len(df.columns)} columns")
                    
                    # Detect data source
                    st.session_state.detected_source = detect_data_source(df)
                    st.info(f"🔍 Detected data source: **{st.session_state.detected_source}**")
                    
                    # Preview data
                    with st.expander("📊 Preview Data (first 5 rows)"):
                        st.dataframe(df.head())
                    
                except Exception as e:
                    st.error(f"Error reading CSV file: {str(e)}")
                    st.info("Please ensure the file is a valid CSV format.")
                    st.session_state.uploaded_file = None
                    st.session_state.df = None
                    return
    
    # STEP 2: Upload Mode Selection (only show if file is uploaded)
    if st.session_state.df is not None and not st.session_state.upload_complete:
        df = st.session_state.df
        detected_source = st.session_state.detected_source
        
        st.header("2️⃣ Upload Mode")
        
        # Detect bulk mode
        is_bulk, num_players = detect_bulk_mode(df, detected_source)
        
        if is_bulk:
            st.info(f"📋 **Bulk upload detected:** Found {num_players} different players in this file")
            upload_mode = st.radio(
                "Choose upload mode:",
                ["Bulk Upload (Multiple Players)", "Single Player (Select One)"],
                help="Bulk mode will create sessions for each player automatically",
                key='upload_mode_radio'
            )
            if st.button("Continue", key='mode_continue'):
                st.session_state.bulk_mode = upload_mode.startswith("Bulk")
                st.session_state.upload_mode_selected = True
                st.rerun()
        else:
            st.info("📋 **Single player mode:** This file contains data for one player")
            if st.button("Continue", key='mode_continue_single'):
                st.session_state.bulk_mode = False
                st.session_state.upload_mode_selected = True
                st.rerun()
    
    # STEP 3+: Process based on mode (only show if mode is selected)
    if st.session_state.upload_mode_selected and not st.session_state.upload_complete:
        df = st.session_state.df
        detected_source = st.session_state.detected_source
        bulk_mode = st.session_state.bulk_mode
        
        if bulk_mode:
            # Bulk mode - Steps 3, 4, 5
            st.header("3️⃣ Player Matching")
            st.markdown("The system will match player names from the CSV to your database")
            
            # Get unique players from CSV
            pitcher_col = None
            for col in ['Pitcher Name', 'Pitcher', 'pitcher', 'pitcher_name']:
                if col in df.columns:
                    pitcher_col = col
                    break
            
            # Check for separate First/Last name columns (PitchLogic format)
            if not pitcher_col and 'First Name' in df.columns and 'Last Name' in df.columns:
                df['_pitcher_full_name'] = df['First Name'].astype(str) + ' ' + df['Last Name'].astype(str)
                pitcher_col = '_pitcher_full_name'
            
            if not pitcher_col:
                st.error("❌ Could not find pitcher name column in CSV. Please check your file format.")
                conn.close()
                return
            
            unique_pitchers = df[pitcher_col].unique()
            num_players = len(unique_pitchers)
            
            # Show matching preview
            st.subheader("Player Matching Preview")
            match_data = []
            has_duplicates = False
            
            for pitcher_name in unique_pitchers:
                # Try to get external ID
                pitcher_rows = df[df[pitcher_col] == pitcher_name]
                first_row = pitcher_rows.iloc[0]
                external_id = extract_external_id(first_row, detected_source)
                
                matched_id, match_info = match_player_name(
                    pitcher_name, players, external_id, detected_source
                )
                
                matched_player = None
                if matched_id:
                    matched_player = next((p for p in players if p['player_id'] == matched_id), None)
                
                # Build match display
                if matched_player:
                    display_name = format_player_display(matched_player, show_ids=bool(external_id))
                    status = '✅ Match'
                    if match_info and match_info.get('has_duplicates'):
                        status = '⚠️ Multiple Matches'
                        has_duplicates = True
                else:
                    display_name = '❌ Not Found'
                    status = '❌ No Match'
                
                match_entry = {
                    'CSV Name': pitcher_name,
                    'Matched Player': display_name,
                    'Status': status
                }
                
                if external_id:
                    match_entry['External ID'] = external_id
                
                match_data.append(match_entry)
            
            match_df = pd.DataFrame(match_data)
            st.dataframe(match_df, width=True)
            
            if has_duplicates:
                st.warning("⚠️ **Duplicate name warning:** Some CSV names matched multiple players in your database. The system will use the best match based on name similarity and external IDs. Review the upload summary carefully.")
            
            unmatched_count = len([m for m in match_data if m['Status'] == '❌ No Match'])
            
            if unmatched_count > 0:
                st.warning(f"⚠️ {unmatched_count} players could not be matched")
                
                # Show which players will need to be created
                unmatched_names = [m['CSV Name'] for m in match_data if m['Status'] == '❌ No Match']
                with st.expander("📋 Players not found in database", expanded=True):
                    for name in unmatched_names:
                        st.write(f"• {name}")
                
                auto_create = st.checkbox(
                    "Automatically create new players for unmatched names",
                    value=True,
                    help="New players will be created with 'R' (right-handed) as default"
                )
                
                # Show what will happen
                if auto_create:
                    st.info(f"✅ **Ready to create:** {unmatched_count} new player(s) will be automatically created during upload")
                else:
                    st.error(f"⚠️ **Warning:** {unmatched_count} player(s) will be skipped. Their pitches will NOT be uploaded.")
            else:
                st.success("✅ All players matched successfully!")
                auto_create = False
            
            # Session configuration for bulk
            st.header("4️⃣ Session Configuration")
            
            # Ask if location and session type are the same
            st.subheader("Session Details")
            same_session_info = st.checkbox(
                "All entries have the same location and session type",
                value=True,
                help="If checked, you'll specify one location and session type for all pitches"
            )
            
            bulk_location = None
            bulk_session_type = None
            
            if same_session_info:
                col1, col2 = st.columns(2)
                with col1:
                    # Get existing locations
                    locations = get_locations(conn)
                    location_options = ["-- No location --"] + [loc['location_name'] for loc in locations] + ["➕ Add new location..."]
                    
                    selected_location = st.selectbox(
                        "Location",
                        location_options,
                        key="bulk_location_select"
                    )
                    
                    if selected_location == "➕ Add new location...":
                        new_location = st.text_input(
                            "Enter new location name:",
                            key="bulk_new_location",
                            placeholder="e.g., Main Field, Training Facility"
                        )
                        if new_location and new_location.strip():
                            # Create the location in the database
                            try:
                                create_location(conn, new_location.strip())
                                st.success(f"✅ Added location: {new_location.strip()}")
                                bulk_location = new_location.strip()
                            except Exception as e:
                                if "Duplicate entry" in str(e):
                                    st.info("This location already exists in the list")
                                    bulk_location = new_location.strip()
                                else:
                                    st.error(f"Error adding location: {str(e)}")
                    elif selected_location != "-- No location --":
                        bulk_location = selected_location
                    
                with col2:
                    bulk_session_type = st.selectbox(
                        "Session Type",
                        ["Bullpen", "Live BP", "Simulated Game", "Long Toss", "Other"],
                        key="bulk_session_type"
                    )
            else:
                st.info("Sessions will be created with default values. You can edit them later in the database.")
            
            col1, col2 = st.columns(2)
            
            with col1:
                data_sources = get_data_sources(conn)
                source_options = {ds['source_name']: ds['source_id'] for ds in data_sources}
                
                default_idx = 0
                if detected_source in source_options:
                    default_idx = list(source_options.keys()).index(detected_source)
                
                data_source = st.selectbox(
                    "Data Source",
                    list(source_options.keys()),
                    index=default_idx
                )
            
            with col2:
                st.info("Sessions will be created automatically for each player")
            
            # Upload buttons
            st.header("5️⃣ Upload Data")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                process_button = st.button("🚀 Process Bulk Upload", type="primary", width=True)
            with col2:
                cancel_button = st.button("❌ Cancel", width=True)
            
            if cancel_button:
                st.session_state.upload_complete = True
                st.rerun()
            
            if process_button:
                with st.spinner(f"Processing {num_players} players and uploading to database..."):
                    success, message, stats = process_csv(
                        df, None, data_source, None, st.session_state.uploaded_file,
                        bulk_mode=True,
                        auto_create_players=auto_create,
                        players_list=players,
                        bulk_location=bulk_location if same_session_info else None,
                        bulk_session_type=bulk_session_type if same_session_info else None
                    )
                    
                    if success:
                        # Store summary in session state
                        summary_data = {
                            'message': message,
                            'stats': stats,
                            'mode': 'bulk'
                        }
                        st.session_state.upload_summary = summary_data
                        st.session_state.upload_complete = True
                        st.rerun()
                    else:
                        st.error(f"Upload failed: {message}")
        
        else:
            # Single player mode - Steps 3, 4, 5
            st.header("3️⃣ Select Player")
            
            if not players:
                st.warning("No active players found. Please add players to the database first.")
                conn.close()
                return
            
            # Create searchable player list
            player_options = {f"{p['player_name']} ({p['graduation_year']})": p['player_id'] 
                             for p in players}
            
            # Type-ahead search box with live filtering
            col1, col2 = st.columns([4, 1])
            
            with col1:
                search_term = st.text_input(
                    "🔍 Search for a player",
                    placeholder="Type name, graduation year, or any text to filter...",
                    key="player_search",
                    help="Start typing to see matching players below"
                )
            
            with col2:
                if search_term and st.button("✖ Clear", key="clear_search"):
                    st.session_state.player_search = ""
                    st.rerun()
            
            # Filter players based on search
            if search_term:
                filtered_players = [name for name in player_options.keys() 
                                   if search_term.lower() in name.lower()]
                # Show match count
                if filtered_players:
                    st.info(f"✓ Found {len(filtered_players)} player(s) matching '{search_term}'")
                else:
                    st.warning(f"No players found matching '{search_term}'. Try a different search.")
            else:
                filtered_players = list(player_options.keys())
                st.caption(f"💡 {len(filtered_players)} total players available - type above to filter")
            
            # Show filtered results
            if filtered_players:
                selected_player = st.selectbox(
                    "Select player",
                    filtered_players,
                    key="player_select"
                )
                player_id = player_options[selected_player]
            else:
                st.warning(f"No players found matching '{search_term}'")
                return
            
            # Step 4: Configure Session
            st.header("4️⃣ Session Details")
            
            col1, col2 = st.columns(2)
            
            with col1:
                session_date = st.date_input("Session Date", value=datetime.now())
                session_type = st.selectbox(
                    "Session Type",
                    ["Bullpen", "Live BP", "Simulated Game", "Long Toss", "Other"]
                )
            
            with col2:
                data_sources = get_data_sources(conn)
                source_options = {ds['source_name']: ds['source_id'] for ds in data_sources}
                
                # Pre-select detected source if found
                default_idx = 0
                if detected_source in source_options:
                    default_idx = list(source_options.keys()).index(detected_source)
                
                data_source = st.selectbox(
                    "Data Source",
                    list(source_options.keys()),
                    index=default_idx
                )
            
            # Location selector with dropdown
            locations = get_locations(conn)
            location_options = ["-- No location --"] + [loc['location_name'] for loc in locations] + ["➕ Add new location..."]
            
            selected_location = st.selectbox(
                "Location",
                location_options,
                key="single_location_select"
            )
            
            location = None
            if selected_location == "➕ Add new location...":
                new_location = st.text_input(
                    "Enter new location name:",
                    key="single_new_location",
                    placeholder="e.g., Main Field, Training Facility"
                )
                if new_location and new_location.strip():
                    # Create the location in the database
                    try:
                        create_location(conn, new_location.strip())
                        st.success(f"✅ Added location: {new_location.strip()}")
                        location = new_location.strip()
                    except Exception as e:
                        if "Duplicate entry" in str(e):
                            st.info("This location already exists in the list")
                            location = new_location.strip()
                        else:
                            st.error(f"Error adding location: {str(e)}")
            elif selected_location != "-- No location --":
                location = selected_location
            
            session_focus = st.text_area("Session Focus (optional)", 
                                        placeholder="What was the focus of this session?")
            
            # Step 5: Process and Upload
            st.header("5️⃣ Upload Data")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                process_button = st.button("🚀 Process and Upload", type="primary", width=True)
            with col2:
                cancel_button = st.button("❌ Cancel", width=True)
            
            if cancel_button:
                st.session_state.upload_complete = True
                st.rerun()
            
            if process_button:
                with st.spinner("Processing CSV and uploading to database..."):
                    # Create training session
                    session_id = create_training_session(
                        conn, player_id, session_date, session_type, 
                        source_options[data_source], len(df), 
                        location if location else "", None, session_focus
                    )
                    
                    # Process CSV
                    success, message, stats = process_csv(
                        df, player_id, data_source, session_id, st.session_state.uploaded_file,
                        bulk_mode=False
                    )
                    
                    if success:
                        # Store summary in session state
                        summary_data = {
                            'message': message,
                            'stats': stats,
                            'mode': 'single',
                            'player_name': selected_player,
                            'session_id': session_id
                        }
                        st.session_state.upload_summary = summary_data
                        st.session_state.upload_complete = True
                        st.rerun()
                    else:
                        st.error(f"Upload failed: {message}")
    
    # Show upload summary and "Upload Another File" button after completion
    if st.session_state.upload_complete and st.session_state.upload_summary:
        st.success("✅ Upload completed successfully!")
        st.balloons()
        
        summary = st.session_state.upload_summary
        
        # Display summary based on mode
        if summary['mode'] == 'bulk':
            st.header("📊 Upload Summary")
            st.info(summary['message'])
            
            if summary['stats'].get('players_processed'):
                st.subheader("Results by Player")
                summary_data = []
                for pitcher_name, info in summary['stats']['players_processed'].items():
                    summary_data.append({
                        'Player': pitcher_name,
                        'Pitches Uploaded': info['pitches'],
                        'Session ID': info['session_id'],
                        'Player ID': info['player_id']
                    })
                summary_df = pd.DataFrame(summary_data)
                st.dataframe(summary_df, width=True)
                
                # Show totals
                total_pitches = summary['stats'].get('inserted', 0)
                total_players = len(summary['stats']['players_processed'])
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Pitches", total_pitches)
                with col2:
                    st.metric("Players Processed", total_players)
                with col3:
                    st.metric("Sessions Created", summary['stats'].get('sessions_created', 0))
        
        else:  # single player mode
            st.header("📊 Upload Summary")
            st.info(summary['message'])
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Player", summary['player_name'])
                st.metric("Pitches Uploaded", summary['stats'].get('inserted', 0))
            with col2:
                st.metric("Session ID", summary['session_id'])
                if summary['stats'].get('skipped', 0) > 0:
                    st.metric("Pitches Skipped", summary['stats']['skipped'])
        
        st.markdown("---")
        if st.button("📁 Upload Another File", type="primary", width=True):
            # Clear all session state
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    
    conn.close()

if __name__ == "__main__":
    main()
