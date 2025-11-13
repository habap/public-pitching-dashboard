"""
Session Detail Page
View detailed information about a specific training session
"""

import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# Page configuration
st.set_page_config(
    page_title="Session Detail",
    page_icon="📋",
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

def get_all_sessions(conn):
    """Get all training sessions"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ts.session_id, ts.session_date, ts.session_type,
               ts.location,
               CONCAT(p.first_name, ' ', p.last_name) as player_name,
               CONCAT(c.first_name, ' ', c.last_name) as coach_name,
               COUNT(pd.pitch_id) as pitch_count
        FROM training_sessions ts
        JOIN players p ON ts.player_id = p.player_id
        LEFT JOIN coaches c ON ts.coach_id = c.coach_id
        LEFT JOIN pitch_data pd ON ts.session_id = pd.session_id
        GROUP BY ts.session_id, ts.session_date, ts.session_type,
                 ts.location, p.first_name, p.last_name, 
                 c.first_name, c.last_name
        ORDER BY ts.session_date DESC, ts.session_id DESC
    """)
    return cursor.fetchall()

def get_session_details(conn, session_id):
    """Get detailed session information"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ts.session_id, ts.player_id, ts.coach_id, ts.session_date,
               ts.session_type, ts.location, ts.session_focus, 
               ts.duration_minutes, ts.coach_notes, ts.source_id, ts.created_at,
               CONCAT(p.first_name, ' ', p.last_name) as player_name,
               p.graduation_year, p.throws_hand,
               CONCAT(c.first_name, ' ', c.last_name) as coach_name,
               c.email as coach_email, c.phone as coach_phone,
               ds.source_name,
               COUNT(pd.pitch_id) as total_pitches,
               AVG(pd.release_speed) as avg_velocity,
               MAX(pd.release_speed) as max_velocity,
               MIN(pd.release_speed) as min_velocity,
               AVG(pd.spin_rate) as avg_spin,
               MAX(pd.spin_rate) as max_spin
        FROM training_sessions ts
        JOIN players p ON ts.player_id = p.player_id
        LEFT JOIN coaches c ON ts.coach_id = c.coach_id
        LEFT JOIN data_sources ds ON ts.source_id = ds.source_id
        LEFT JOIN pitch_data pd ON ts.session_id = pd.session_id
        WHERE ts.session_id = %s
        GROUP BY ts.session_id, ts.player_id, ts.coach_id, ts.session_date,
                 ts.session_type, ts.location, ts.session_focus, 
                 ts.duration_minutes, ts.coach_notes, ts.source_id, ts.created_at,
                 p.first_name, p.last_name, p.graduation_year, p.throws_hand,
                 c.first_name, c.last_name, c.email, c.phone, c.coach_id,
                 ds.source_id, ds.source_name
    """, (session_id,))
    return cursor.fetchone()

def get_session_pitches(conn, session_id):
    """Get all pitches for a session"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT pd.*
        FROM pitch_data pd
        WHERE pd.session_id = %s
        ORDER BY pd.pitch_number
    """, (session_id,))
    return cursor.fetchall()

def main():
    st.title("📋 Session Detail")
    
    conn = get_db_connection()
    if not conn:
        st.error("Could not connect to database")
        return
    
    # Check if a session was selected from another page
    selected_session_id = st.session_state.get('selected_session_id')
    
    # Session selection
    sessions = get_all_sessions(conn)
    
    if not sessions:
        st.warning("No training sessions found in database")
        conn.close()
        return
    
    # Create session options for dropdown
    session_options = {
        f"{s['session_date'].strftime('%m/%d/%Y')} - {s['player_name']} - {s['session_type']} ({s['pitch_count']} pitches)": s['session_id']
        for s in sessions
    }
    
    # If there's a pre-selected session, use it as default
    if selected_session_id and selected_session_id in session_options.values():
        # Find the index of the selected session
        default_index = list(session_options.values()).index(selected_session_id)
    else:
        default_index = 0
    
    selected_session = st.selectbox(
        "Select a training session",
        list(session_options.keys()),
        index=default_index
    )
    
    session_id = session_options[selected_session]
    
    # Clear the selected session from state after using it
    if 'selected_session_id' in st.session_state:
        del st.session_state['selected_session_id']
    
    # Get session details
    session = get_session_details(conn, session_id)
    
    if not session:
        st.error("Session not found")
        conn.close()
        return
    
    # Session Header
    st.markdown("---")
    
    # Add navigation breadcrumb
    col_nav, col_space = st.columns([3, 1])
    with col_nav:
        if st.button(f"← Back to {session['player_name']}'s Profile", key="back_to_player"):
            st.session_state['selected_player_id'] = session['player_id']
            st.switch_page("pages/1_Player_Profile.py")
    
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Player", session['player_name'])
        if session['throws_hand']:
            st.caption(f"Throws: {session['throws_hand']}")
        if session['graduation_year']:
            st.caption(f"Class of {session['graduation_year']}")
    
    with col2:
        st.metric("Date", session['session_date'].strftime('%m/%d/%Y'))
        st.metric("Session Type", session['session_type'])
    
    with col3:
        if session['coach_name']:
            st.metric("Coach", session['coach_name'])
        if session['location']:
            st.metric("Location", session['location'])
    
    with col4:
        st.metric("Total Pitches", session['total_pitches'] or 0)
        if session['source_name']:
            st.metric("Data Source", session['source_name'])
    
    # Session Info
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Session Information")
        if session.get('session_focus'):
            st.write(f"**Focus:** {session['session_focus']}")
        if session.get('duration_minutes'):
            st.write(f"**Duration:** {session['duration_minutes']} minutes")
        if session.get('notes'):
            st.write(f"**Notes:** {session['notes']}")
    
    with col2:
        st.subheader("Quick Stats")
        if session['avg_velocity']:
            col_a, col_b = st.columns(2)
            with col_a:
                st.metric("Avg Velocity", f"{session['avg_velocity']:.1f} mph")
                st.metric("Min Velocity", f"{session['min_velocity']:.1f} mph")
            with col_b:
                st.metric("Max Velocity", f"{session['max_velocity']:.1f} mph")
                if session['avg_spin']:
                    st.metric("Avg Spin", f"{session['avg_spin']:.0f} rpm")
    
    # Tabs for different views
    tab1, tab2 = st.tabs(["⚾ Pitch List", "📊 Session Summary"])
    
    with tab1:
        st.header("Pitch-by-Pitch Data")
        
        pitches = get_session_pitches(conn, session_id)
        
        if pitches:
            st.write(f"**Total Pitches:** {len(pitches)}")
            st.markdown("---")
            
            # Display pitches in rows with View buttons
            for pitch in pitches:
                with st.container():
                    col1, col2, col3, col4, col5 = st.columns([1, 2, 2, 3, 1])
                    
                    with col1:
                        st.write(f"**Pitch #{pitch['pitch_number']}**")
                    
                    with col2:
                        if pitch['release_speed']:
                            st.write(f"**{pitch['release_speed']:.1f} mph**")
                        else:
                            st.write("No velocity")
                    
                    with col3:
                        metrics = []
                        if pitch['spin_rate']:
                            metrics.append(f"Spin: {pitch['spin_rate']:.0f} rpm")
                        if pitch['spin_axis']:
                            metrics.append(f"Axis: {pitch['spin_axis']:.0f}°")
                        st.write(" | ".join(metrics) if metrics else "No spin data")
                    
                    with col4:
                        movement = []
                        if pitch['horizontal_break']:
                            movement.append(f"H: {pitch['horizontal_break']:.1f}\"")
                        if pitch['induced_vertical_break']:
                            movement.append(f"V: {pitch['induced_vertical_break']:.1f}\"")
                        if pitch['release_height']:
                            movement.append(f"Height: {pitch['release_height']:.2f}'")
                        st.write(" | ".join(movement) if movement else "No movement data")
                    
                    with col5:
                        if st.button("View", key=f"pitch_{pitch['pitch_id']}", use_container_width=True):
                            # Store both pitch and session info in session state
                            st.session_state['selected_pitch_id'] = pitch['pitch_id']
                            st.session_state['selected_session_id'] = session_id
                            st.session_state['session_player_name'] = session['player_name']
                            st.session_state['session_player_id'] = session['player_id']
                            # Navigate to Pitch Detail page
                            st.switch_page("pages/3_Pitch_Detail.py")
                    
                    st.divider()
            
            # Download all pitches as CSV
            pitch_data = []
            for pitch in pitches:
                pitch_data.append({
                    'Pitch #': pitch['pitch_number'],
                    'Velocity': f"{pitch['release_speed']:.1f}" if pitch['release_speed'] else 'N/A',
                    'Spin Rate': f"{pitch['spin_rate']:.0f}" if pitch['spin_rate'] else 'N/A',
                    'Spin Axis': f"{pitch['spin_axis']:.0f}°" if pitch['spin_axis'] else 'N/A',
                    'H Break': f"{pitch['horizontal_break']:.1f}\"" if pitch['horizontal_break'] else 'N/A',
                    'V Break': f"{pitch['induced_vertical_break']:.1f}\"" if pitch['induced_vertical_break'] else 'N/A',
                    'Release Height': f"{pitch['release_height']:.2f}'" if pitch['release_height'] else 'N/A',
                    'Extension': f"{pitch['release_extension']:.2f}'" if pitch['release_extension'] else 'N/A',
                    'Release Side': f"{pitch['release_side']:.2f}'" if pitch['release_side'] else 'N/A',
                })
            
            df = pd.DataFrame(pitch_data)
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download All Pitches as CSV",
                data=csv,
                file_name=f"session_{session_id}_pitches.csv",
                mime="text/csv"
            )
        else:
            st.info("No pitch data recorded for this session")
    
    with tab2:
        st.header("Session Summary & Analytics")
        
        pitches = get_session_pitches(conn, session_id)
        
        if pitches and len(pitches) > 0:
            df = pd.DataFrame(pitches)
            
            # Velocity stats
            if 'release_speed' in df.columns and df['release_speed'].notna().any():
                st.subheader("Velocity Analysis")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Average", f"{df['release_speed'].mean():.1f} mph")
                with col2:
                    st.metric("Maximum", f"{df['release_speed'].max():.1f} mph")
                with col3:
                    st.metric("Minimum", f"{df['release_speed'].min():.1f} mph")
                with col4:
                    st.metric("Std Dev", f"{df['release_speed'].std():.2f} mph")
                
                # Velocity progression chart
                st.subheader("Velocity Throughout Session")
                fig = px.line(df, x='pitch_number', y='release_speed',
                            title='Velocity by Pitch Number',
                            labels={'pitch_number': 'Pitch Number', 'release_speed': 'Velocity (mph)'})
                fig.add_hline(y=df['release_speed'].mean(), line_dash="dash", 
                            line_color="red", annotation_text="Average")
                st.plotly_chart(fig, use_container_width=True)
                
                # Velocity distribution
                fig = px.histogram(df, x='release_speed', nbins=20,
                                 title='Velocity Distribution',
                                 labels={'release_speed': 'Velocity (mph)', 'count': 'Number of Pitches'})
                st.plotly_chart(fig, use_container_width=True)
            
            # Spin rate stats
            if 'spin_rate' in df.columns and df['spin_rate'].notna().any():
                st.subheader("Spin Rate Analysis")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Average", f"{df['spin_rate'].mean():.0f} rpm")
                with col2:
                    st.metric("Maximum", f"{df['spin_rate'].max():.0f} rpm")
                with col3:
                    st.metric("Minimum", f"{df['spin_rate'].min():.0f} rpm")
                with col4:
                    st.metric("Std Dev", f"{df['spin_rate'].std():.0f} rpm")
                
                # Spin rate progression
                fig = px.line(df, x='pitch_number', y='spin_rate',
                            title='Spin Rate by Pitch Number',
                            labels={'pitch_number': 'Pitch Number', 'spin_rate': 'Spin Rate (rpm)'})
                fig.add_hline(y=df['spin_rate'].mean(), line_dash="dash", 
                            line_color="red", annotation_text="Average")
                st.plotly_chart(fig, use_container_width=True)
            
            # Movement analysis
            if 'horizontal_break' in df.columns and 'induced_vertical_break' in df.columns:
                df_movement = df[df['horizontal_break'].notna() & df['induced_vertical_break'].notna()]
                if len(df_movement) > 0:
                    st.subheader("Movement Analysis")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Horizontal Break**")
                        st.metric("Average", f"{df['horizontal_break'].mean():.2f}\"")
                        st.metric("Range", f"{df['horizontal_break'].min():.2f}\" to {df['horizontal_break'].max():.2f}\"")
                    
                    with col2:
                        st.write("**Induced Vertical Break**")
                        st.metric("Average", f"{df['induced_vertical_break'].mean():.2f}\"")
                        st.metric("Range", f"{df['induced_vertical_break'].min():.2f}\" to {df['induced_vertical_break'].max():.2f}\"")
                    
                    # Movement chart
                    st.subheader("Pitch Movement Profile")
                    fig = px.scatter(df_movement, x='horizontal_break', y='induced_vertical_break',
                                   title='Movement Chart',
                                   labels={'horizontal_break': 'Horizontal Break (in)', 
                                          'induced_vertical_break': 'Induced Vertical Break (in)'},
                                   hover_data=['pitch_number', 'release_speed'],
                                   opacity=0.7)
                    fig.add_hline(y=0, line_dash="dash", line_color="gray")
                    fig.add_vline(x=0, line_dash="dash", line_color="gray")
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No pitch data available for analytics")
    
    conn.close()

if __name__ == "__main__":
    main()
