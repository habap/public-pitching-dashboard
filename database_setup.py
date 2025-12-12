"""
Database Setup and Migration Script
Creates all necessary tables for the pitching analytics dashboard
Run this once to set up your SQLite database
"""

import sqlite3
import os

DB_PATH = "pitching_analytics.db"

def create_database():
    """Create all necessary tables for the pitching analytics dashboard"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Creating/updating database tables...")
    
    # 1. Players table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            parent_email TEXT,
            graduation_year INTEGER,
            throws_hand TEXT,
            bats_hand TEXT,
            is_active INTEGER DEFAULT 1,
            rapsodo_player_id TEXT,
            pitchlogic_player_id TEXT,
            trackman_player_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Players table created")
    
    # 2. Coaches table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS coaches (
            coach_id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            organization TEXT,
            title TEXT,
            certification TEXT,
            bio TEXT,
            website TEXT,
            social_media TEXT,
            photo BLOB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Coaches table created")
    
    # 3. Locations table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            location_id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Locations table created")
    
    # 4. Data sources table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_sources (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT NOT NULL UNIQUE,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Data sources table created")
    
    # Insert default data sources if they don't exist
    sources = [
        ('Rapsodo', 'Rapsodo pitch tracking system'),
        ('PitchLogic', 'PitchLogic ball sensor tracking'),
        ('Trackman', 'Trackman radar tracking system'),
        ('Manual', 'Manually entered data')
    ]
    
    for source_name, description in sources:
        cursor.execute("""
            INSERT OR IGNORE INTO data_sources (source_name, description)
            VALUES (?, ?)
        """, (source_name, description))
    print("✓ Data sources populated")
    
    # 5. Training sessions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS training_sessions (
            session_id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            coach_id INTEGER,
            data_source_id INTEGER,
            session_date DATE NOT NULL,
            session_type TEXT,
            location TEXT,
            location_id INTEGER,
            session_focus TEXT,
            duration_minutes INTEGER,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (player_id) REFERENCES players(player_id),
            FOREIGN KEY (coach_id) REFERENCES coaches(coach_id),
            FOREIGN KEY (data_source_id) REFERENCES data_sources(source_id),
            FOREIGN KEY (location_id) REFERENCES locations(location_id)
        )
    """)
    print("✓ Training sessions table created")
    
    # 6. Pitch data table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pitch_data (
            pitch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            pitch_number INTEGER,
            pitch_type TEXT,
            pitch_result TEXT,
            release_speed REAL,
            spin_rate REAL,
            spin_axis REAL,
            spin_efficiency REAL,
            horizontal_break REAL,
            induced_vertical_break REAL,
            vertical_break REAL,
            release_height REAL,
            release_side REAL,
            release_extension REAL,
            release_angle REAL,
            vertical_approach_angle REAL,
            gyro_degree REAL,
            plate_location_x REAL,
            plate_location_z REAL,
            timestamp TIMESTAMP,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (session_id) REFERENCES training_sessions(session_id)
        )
    """)
    print("✓ Pitch data table created")
    
    # Create indexes for better query performance
    indexes = [
        ("idx_players_active", "players", "is_active"),
        ("idx_sessions_player", "training_sessions", "player_id"),
        ("idx_sessions_date", "training_sessions", "session_date"),
        ("idx_sessions_coach", "training_sessions", "coach_id"),
        ("idx_pitch_session", "pitch_data", "session_id"),
        ("idx_pitch_type", "pitch_data", "pitch_type"),
    ]
    
    for idx_name, table, column in indexes:
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column})
        """)
    print("✓ Indexes created")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Database setup complete! File: {DB_PATH}")
    print(f"   Location: {os.path.abspath(DB_PATH)}")

def check_existing_data():
    """Check if database has existing data"""
    if not os.path.exists(DB_PATH):
        print(f"No existing database found at {DB_PATH}")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check for data in key tables
        cursor.execute("SELECT COUNT(*) FROM players")
        player_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM training_sessions")
        session_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM pitch_data")
        pitch_count = cursor.fetchone()[0]
        
        if player_count > 0 or session_count > 0 or pitch_count > 0:
            print(f"\n📊 Found existing data:")
            print(f"   Players: {player_count}")
            print(f"   Sessions: {session_count}")
            print(f"   Pitches: {pitch_count}")
            return True
    except:
        pass
    finally:
        conn.close()
    
    return False

def add_sample_coach():
    """Add a sample coach for testing"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if any coaches exist
    cursor.execute("SELECT COUNT(*) FROM coaches")
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
            INSERT INTO coaches (first_name, last_name, email, phone, organization, title)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("Sample", "Coach", "coach@example.com", "555-1234", "Test Organization", "Head Coach"))
        conn.commit()
        print("\n✓ Added sample coach (you can edit or delete this later)")
    
    conn.close()

if __name__ == "__main__":
    print("=" * 60)
    print("Pitching Analytics Database Setup")
    print("=" * 60)
    print()
    
    # Check for existing data
    has_data = check_existing_data()
    
    # Create/update tables
    create_database()
    
    # Optionally add sample data
    if not has_data:
        print("\nNo existing data found.")
        response = input("Would you like to add a sample coach for testing? (y/n): ").strip().lower()
        if response == 'y':
            add_sample_coach()
    
    print("\n" + "=" * 60)
    print("Setup complete! You can now run your Streamlit app.")
    print("=" * 60)
