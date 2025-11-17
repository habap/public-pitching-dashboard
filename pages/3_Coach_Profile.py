"""
Coach Profile Page
View coach information including photo, contact details, and associated sessions
"""

import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import base64
from io import BytesIO
from PIL import Image

# Page configuration
st.set_page_config(
    page_title="Coach Profile",
    page_icon="👨‍🏫",
    layout="wide"
)

# Database connection configuration
DB_CONFIG = {
    'host': st.secrets.get("DB_HOST", "localhost"),
    'database': st.secrets.get("DB_NAME", "pitching_dev"),
    'user': st.secrets.get("DB_USER", "root"),
    'password': st.secrets.get("DB_PASSWORD", ""),
    'port': st.secrets.get("DB_PORT", 3306)
}

def get_db_connection():
    """Create database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        st.error(f"Error connecting to MySQL: {e}")
        return None

def get_all_coaches(conn):
    """Get all coaches"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT c.coach_id, 
               CONCAT(c.first_name, ' ', c.last_name) as coach_name,
               c.organization,
               COUNT(DISTINCT ts.session_id) as session_count,
               COUNT(DISTINCT ts.player_id) as player_count
        FROM coaches c
        LEFT JOIN training_sessions ts ON c.coach_id = ts.coach_id
        GROUP BY c.coach_id
        ORDER BY c.last_name, c.first_name
    """)
    return cursor.fetchall()

def get_coach_details(conn, coach_id):
    """Get detailed coach information"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT c.*,
               COUNT(DISTINCT ts.session_id) as total_sessions,
               COUNT(DISTINCT ts.player_id) as total_players,
               MIN(ts.session_date) as first_session,
               MAX(ts.session_date) as last_session
        FROM coaches c
        LEFT JOIN training_sessions ts ON c.coach_id = ts.coach_id
        WHERE c.coach_id = %s
        GROUP BY c.coach_id
    """, (coach_id,))
    return cursor.fetchone()

def get_coach_sessions(conn, coach_id):
    """Get all sessions for a coach"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ts.session_id, ts.session_date, ts.session_type,
               ts.location, ts.duration_minutes,
               CONCAT(p.first_name, ' ', p.last_name) as player_name,
               p.player_id,
               COUNT(pd.pitch_id) as pitch_count,
               AVG(pd.release_speed) as avg_velocity,
               MAX(pd.release_speed) as max_velocity
        FROM training_sessions ts
        JOIN players p ON ts.player_id = p.player_id
        LEFT JOIN pitch_data pd ON ts.session_id = pd.session_id
        WHERE ts.coach_id = %s
        GROUP BY ts.session_id
        ORDER BY ts.session_date DESC
    """, (coach_id,))
    return cursor.fetchall()

def get_coach_players(conn, coach_id):
    """Get all players who have worked with this coach"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT DISTINCT p.player_id,
               CONCAT(p.first_name, ' ', p.last_name) as player_name,
               p.graduation_year,
               COUNT(DISTINCT ts.session_id) as session_count,
               MIN(ts.session_date) as first_session,
               MAX(ts.session_date) as last_session
        FROM players p
        JOIN training_sessions ts ON p.player_id = ts.player_id
        WHERE ts.coach_id = %s
        GROUP BY p.player_id
        ORDER BY session_count DESC
    """, (coach_id,))
    return cursor.fetchall()

def update_coach_info(conn, coach_id, field, value):
    """Update coach information"""
    cursor = conn.cursor()
    query = f"UPDATE coaches SET {field} = %s WHERE coach_id = %s"
    cursor.execute(query, (value, coach_id))
    conn.commit()
    return True

def upload_coach_photo(conn, coach_id, photo_data):
    """Upload coach photo to database"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE coaches SET photo = %s WHERE coach_id = %s
    """, (photo_data, coach_id))
    conn.commit()
    return True

def main():
    st.title("👨‍🏫 Coach Profile")
    
    conn = get_db_connection()
    if not conn:
        st.error("Could not connect to database")
        return
    
    # Coach selection
    coaches = get_all_coaches(conn)
    
    if not coaches:
        st.warning("No coaches found in database")
        st.info("Coaches are automatically linked when you assign them to sessions")
        conn.close()
        return
    
    coach_options = {}
    for c in coaches:
        display_name = c['coach_name']
        if c.get('organization'):
            display_name += f" ({c['organization']})"
        coach_options[display_name] = c['coach_id']
    
    selected_coach = st.selectbox("Select a coach", list(coach_options.keys()))
    coach_id = coach_options[selected_coach]
    
    # Get coach details
    coach = get_coach_details(conn, coach_id)
    
    if not coach:
        st.error("Coach not found")
        conn.close()
        return
    
    st.markdown("---")
    
    # Coach Header with Photo
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Display coach photo
        if coach.get('photo'):
            try:
                image = Image.open(BytesIO(coach['photo']))
                st.image(image, caption=f"{coach['first_name']} {coach['last_name']}", width='stretch')
            except:
                st.image("https://via.placeholder.com/300x400?text=No+Photo", width='stretch')
        else:
            st.image("https://via.placeholder.com/300x400?text=No+Photo", width='stretch')
        
        # Photo upload
        with st.expander("📸 Upload Photo"):
            uploaded_photo = st.file_uploader("Choose a photo", type=['jpg', 'jpeg', 'png'])
            if uploaded_photo and st.button("Upload Photo"):
                photo_bytes = uploaded_photo.read()
                upload_coach_photo(conn, coach_id, photo_bytes)
                st.success("Photo uploaded successfully!")
                st.rerun()
    
    with col2:
        # Coach Name and Title
        st.header(f"{coach['first_name']} {coach['last_name']}")
        
        if coach.get('title'):
            st.subheader(coach['title'])
        
        # Organization
        col_a, col_b = st.columns(2)
        with col_a:
            if coach.get('organization'):
                st.write(f"**Organization:** {coach['organization']}")
        with col_b:
            if coach.get('certification'):
                st.write(f"**Certification:** {coach['certification']}")
        
        # Bio
        if coach.get('bio'):
            st.write(coach['bio'])
        
        st.markdown("---")
        
        # Contact Information - Clickable
        st.subheader("📞 Contact Information")
        
        col_a, col_b = st.columns(2)
        
        with col_a:
            if coach.get('email'):
                st.markdown(f"**Email:** [{coach['email']}](mailto:{coach['email']})")
            else:
                st.write("**Email:** Not provided")
            
            if coach.get('phone'):
                # Make phone clickable
                st.markdown(f"**Phone:** [{coach['phone']}](tel:{coach['phone']})")
            else:
                st.write("**Phone:** Not provided")
        
        with col_b:
            if coach.get('website'):
                # Make website clickable - ensure it has http/https
                website = coach['website']
                if not website.startswith('http'):
                    website = f"https://{website}"
                st.markdown(f"**Website:** [Visit Website]({website})")
            else:
                st.write("**Website:** Not provided")
            
            if coach.get('social_media'):
                st.write(f"**Social Media:** {coach['social_media']}")
        
        st.markdown("---")
        
        # Statistics
        st.subheader("📊 Statistics")
        col_a, col_b, col_c, col_d = st.columns(4)
        
        with col_a:
            st.metric("Total Sessions", coach['total_sessions'] or 0)
        with col_b:
            st.metric("Total Players", coach['total_players'] or 0)
        with col_c:
            if coach['first_session']:
                st.metric("First Session", coach['first_session'].strftime('%m/%d/%Y'))
        with col_d:
            if coach['last_session']:
                st.metric("Last Session", coach['last_session'].strftime('%m/%d/%Y'))
    
    st.markdown("---")
    
    # Edit mode
    with st.expander("✏️ Edit Coach Information"):
        st.write("Update coach contact information and details")
        
        col1, col2 = st.columns(2)
        
        with col1:
            new_email = st.text_input("Email", value=coach.get('email') or "", key="edit_email")
            new_phone = st.text_input("Phone", value=coach.get('phone') or "", key="edit_phone")
            new_website = st.text_input("Website", value=coach.get('website') or "", key="edit_website")
        
        with col2:
            new_title = st.text_input("Title", value=coach.get('title') or "", key="edit_title")
            new_organization = st.text_input("Organization", value=coach.get('organization') or "", key="edit_org")
            new_certification = st.text_input("Certification", value=coach.get('certification') or "", key="edit_cert")
        
        new_bio = st.text_area("Bio", value=coach.get('bio') or "", key="edit_bio")
        new_social = st.text_input("Social Media", value=coach.get('social_media') or "", key="edit_social")
        
        if st.button("💾 Save Changes", type="primary"):
            try:
                update_coach_info(conn, coach_id, 'email', new_email)
                update_coach_info(conn, coach_id, 'phone', new_phone)
                update_coach_info(conn, coach_id, 'website', new_website)
                update_coach_info(conn, coach_id, 'title', new_title)
                update_coach_info(conn, coach_id, 'organization', new_organization)
                update_coach_info(conn, coach_id, 'certification', new_certification)
                update_coach_info(conn, coach_id, 'bio', new_bio)
                update_coach_info(conn, coach_id, 'social_media', new_social)
                st.success("✅ Coach information updated successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error updating coach information: {str(e)}")
    
    # Tabs for different views
    tab1, tab2 = st.tabs(["📅 Sessions", "👥 Players"])
    
    with tab1:
        st.header("Training Sessions")
        sessions = get_coach_sessions(conn, coach_id)
        
        if sessions:
            session_data = []
            for session in sessions:
                session_data.append({
                    'Session ID': session['session_id'],
                    'Date': session['session_date'].strftime('%m/%d/%Y'),
                    'Player': session['player_name'],
                    'Type': session['session_type'],
                    'Location': session['location'] or 'N/A',
                    'Duration': f"{session['duration_minutes']} min" if session['duration_minutes'] else 'N/A',
                    'Pitches': session['pitch_count'],
                    'Avg Velo': f"{session['avg_velocity']:.1f}" if session['avg_velocity'] else 'N/A',
                    'Max Velo': f"{session['max_velocity']:.1f}" if session['max_velocity'] else 'N/A',
                })
            
            df = pd.DataFrame(session_data)
            st.dataframe(df, width='stretch', hide_index=True)
            
            st.info("💡 Tip: Go to Session Detail page to view individual session details")
        else:
            st.info("No sessions found for this coach")
    
    with tab2:
        st.header("Players")
        players = get_coach_players(conn, coach_id)
        
        if players:
            for player in players:
                with st.container():
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.subheader(player['player_name'])
                        if player.get('graduation_year'):
                            st.write(f"**Graduation Year:** {player['graduation_year']}")
                    
                    with col2:
                        st.metric("Sessions Together", player['session_count'])
                        if player['first_session']:
                            st.write(f"**First:** {player['first_session'].strftime('%m/%d/%Y')}")
                        if player['last_session']:
                            st.write(f"**Last:** {player['last_session'].strftime('%m/%d/%Y')}")
                    
                    if st.button(f"View Player Profile", key=f"player_{player['player_id']}"):
                        st.info("💡 Go to Player Profile page and select this player")
                    
                    st.markdown("---")
        else:
            st.info("No players associated with this coach yet")
    
    conn.close()

if __name__ == "__main__":
    main()
