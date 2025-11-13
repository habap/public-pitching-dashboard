"""
Session Detail Page
View individual session details including player, coach, location, and all pitches
"""

import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
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

def get_recent_sessions(conn, limit=50):
    """Get recent sessions"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ts.session_id, ts.session_date, ts.session_type,
               CONCAT(p.first_name, ' ', p.last_name) as player_name,
               COUNT(pd.pitch_id) as pitch_count
        FROM training_sessions ts
        JOIN players p ON ts.player_id = p.player_id
        LEFT JOIN pitch_data pd ON ts.session_id = pd.session_id
        GROUP BY ts.session_id
        ORDER BY ts.session_date DESC, ts.session_id DESC
        LIMIT %s
    """, (limit,))
    return cursor.fetchall()

def get_session_details(conn, session_id):
    """Get detailed session information"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ts.*,
               p.first_name, p.last_name, p.throws_hand, p.graduation_year,
               p.email as player_email, p.phone as player_phone,
               CONCAT(c.first_name, ' ', c.last_name) as coach_name,
               c.email as coach_email, c.phone as coach_phone,
               c.organization as coach_organization, c.coach_id,
               ds.source_name,
               COUNT(pd.pitch_id) as total_pitches,
               AVG(pd.release_speed) as avg_velocity,
               MAX(pd.release_speed) as max_velocity,
               MIN(pd.release_speed) as min_velocity,
               AVG(pd.spin_rate) as avg_spin_rate,
               MAX(pd.spin_rate) as max_spin_rate,
               AVG(pd.spin_efficiency) as avg_spin_efficiency
        FROM training_sessions ts
        JOIN players p ON ts.player_id = p.player_id
        LEFT JOIN coaches c ON ts.coach_id = c.coach_id
        JOIN data_sources ds ON ts.source_id = ds.source_id
        LEFT JOIN pitch_data pd ON ts.session_id = pd.session_id
        WHERE ts.session_id = %s
        GROUP BY ts.session_id
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
    
    # Session selection
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Option to search by session ID
        session_id_input = st.number_input("Enter Session ID", min_value=1, step=1, value=None)
    
    with col2:
        st.write("")  # Spacing
        st.write("")  # Spacing
        search_button = st.button("🔍 Load Session", type="primary")
    
    # Show recent sessions
    st.subheader("Recent Sessions")
    recent_sessions = get_recent_sessions(conn)
    
    if recent_sessions:
        session_data = []
        for session in recent_sessions:
            session_data.append({
                'Session ID': session['session_id'],
                'Date': session['session_date'].strftime('%m/%d/%Y'),
                'Player': session['player_name'],
                'Type': session['session_type'],
                'Pitches': session['pitch_count']
            })
        
        df = pd.DataFrame(session_data)
        
        # Use data editor to allow selection
        event = st.dataframe(df, use_container_width=True, hide_index=True, 
                            on_select="rerun", selection_mode="single-row")
        
        # Get selected session
        if event.selection.rows:
            selected_idx = event.selection.rows[0]
            session_id_input = df.iloc[selected_idx]['Session ID']
    
    # Load session if ID provided
    if session_id_input or search_button:
        if not session_id_input:
            st.warning("Please enter a Session ID")
        else:
            session = get_session_details(conn, session_id_input)
            
            if not session:
                st.error(f"Session {session_id_input} not found")
                conn.close()
                return
            
            st.markdown("---")
            st.header(f"Session #{session['session_id']}")
            
            # Session Overview
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Date", session['session_date'].strftime('%m/%d/%Y'))
                st.metric("Type", session['session_type'])
            
            with col2:
                st.metric("Total Pitches", session['total_pitches'])
                if session['duration_minutes']:
                    st.metric("Duration", f"{session['duration_minutes']} min")
            
            with col3:
                if session['avg_velocity']:
                    st.metric("Avg Velocity", f"{session['avg_velocity']:.1f} mph")
                if session['max_velocity']:
                    st.metric("Max Velocity", f"{session['max_velocity']:.1f} mph")
            
            with col4:
                if session['avg_spin_rate']:
                    st.metric("Avg Spin", f"{session['avg_spin_rate']:.0f} rpm")
                if session['max_spin_rate']:
                    st.metric("Max Spin", f"{session['max_spin_rate']:.0f} rpm")
            
            # Player, Coach, Location sections
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.subheader("👤 Player")
                st.write(f"**Name:** {session['first_name']} {session['last_name']}")
                st.write(f"**Throws:** {session['throws_hand']}")
                if session['graduation_year']:
                    st.write(f"**Grad Year:** {session['graduation_year']}")
                if session['player_email']:
                    st.write(f"**Email:** [{session['player_email']}](mailto:{session['player_email']})")
                if session['player_phone']:
                    st.write(f"**Phone:** [{session['player_phone']}](tel:{session['player_phone']})")
            
            with col2:
                st.subheader("👨‍🏫 Coach")
                if session['coach_name']:
                    st.write(f"**Name:** {session['coach_name']}")
                    if session['coach_organization']:
                        st.write(f"**Organization:** {session['coach_organization']}")
                    if session['coach_email']:
                        st.write(f"**Email:** [{session['coach_email']}](mailto:{session['coach_email']})")
                    if session['coach_phone']:
                        st.write(f"**Phone:** [{session['coach_phone']}](tel:{session['coach_phone']})")
                    
                    if session.get('coach_id'):
                        if st.button("View Coach Profile"):
                            st.info("💡 Go to Coach Profile page and select this coach")
                else:
                    st.info("No coach assigned to this session")
            
            with col3:
                st.subheader("📍 Location & Details")
                if session['location']:
                    st.write(f"**Location:** {session['location']}")
                else:
                    st.write("**Location:** Not specified")
                st.write(f"**Data Source:** {session['source_name']}")
                if session['session_focus']:
                    st.write(f"**Focus:** {session['session_focus']}")
            
            # Session notes
            if session.get('notes'):
                with st.expander("📝 Session Notes"):
                    st.write(session['notes'])
            
            st.markdown("---")
            
            # Pitch Data
            st.header("⚾ Pitch Data")
            
            pitches = get_session_pitches(conn, session_id_input)
            
            if pitches:
                # Create tabs for different views
                tab1, tab2, tab3 = st.tabs(["📊 Data Table", "📈 Velocity Chart", "🎯 Movement Plot"])
                
                with tab1:
                    pitch_data = []
                    for pitch in pitches:
                        pitch_data.append({
                            'Pitch #': pitch['pitch_number'],
                            'Velocity': f"{pitch['release_speed']:.1f}" if pitch['release_speed'] else 'N/A',
                            'Spin Rate': f"{pitch['spin_rate']:.0f}" if pitch['spin_rate'] else 'N/A',
                            'Spin Axis': f"{pitch['spin_axis']:.0f}°" if pitch['spin_axis'] else 'N/A',
                            'Spin Eff %': f"{pitch['spin_efficiency']:.1f}" if pitch['spin_efficiency'] else 'N/A',
                            'H Break': f"{pitch['horizontal_break']:.1f}" if pitch['horizontal_break'] else 'N/A',
                            'V Break': f"{pitch['induced_vertical_break']:.1f}" if pitch['induced_vertical_break'] else 'N/A',
                            'Rel Height': f"{pitch['release_height']:.2f}" if pitch['release_height'] else 'N/A',
                            'Rel Side': f"{pitch['release_side']:.2f}" if pitch['release_side'] else 'N/A',
                            'Extension': f"{pitch['release_extension']:.2f}" if pitch['release_extension'] else 'N/A',
                            'Exit Velo': f"{pitch['exit_velocity']:.1f}" if pitch['exit_velocity'] else 'N/A',
                            'Launch Angle': f"{pitch['launch_angle']:.1f}°" if pitch['launch_angle'] else 'N/A',
                        })
                    
                    df_pitches = pd.DataFrame(pitch_data)
                    st.dataframe(df_pitches, use_container_width=True, hide_index=True)
                    
                    # Download button
                    csv = df_pitches.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Session Data as CSV",
                        data=csv,
                        file_name=f"session_{session_id_input}_pitches.csv",
                        mime="text/csv"
                    )
                
                with tab2:
                    # Velocity progression chart
                    df = pd.DataFrame(pitches)
                    if 'release_speed' in df.columns and df['release_speed'].notna().any():
                        fig = px.line(df, x='pitch_number', y='release_speed',
                                    title='Velocity by Pitch Number',
                                    labels={'pitch_number': 'Pitch Number', 'release_speed': 'Velocity (mph)'},
                                    markers=True)
                        
                        # Add average line
                        avg_velo = df['release_speed'].mean()
                        fig.add_hline(y=avg_velo, line_dash="dash", 
                                     annotation_text=f"Avg: {avg_velo:.1f} mph",
                                     line_color="red")
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Velocity distribution histogram
                        fig2 = px.histogram(df, x='release_speed', nbins=20,
                                          title='Velocity Distribution',
                                          labels={'release_speed': 'Velocity (mph)'})
                        st.plotly_chart(fig2, use_container_width=True)
                    else:
                        st.info("No velocity data available for this session")
                
                with tab3:
                    # Movement plot
                    df = pd.DataFrame(pitches)
                    if 'horizontal_break' in df.columns and 'induced_vertical_break' in df.columns:
                        df_movement = df[df['horizontal_break'].notna() & df['induced_vertical_break'].notna()]
                        
                        if len(df_movement) > 0:
                            fig = px.scatter(df_movement, x='horizontal_break', y='induced_vertical_break',
                                           title='Pitch Movement Profile',
                                           labels={'horizontal_break': 'Horizontal Break (in)', 
                                                  'induced_vertical_break': 'Induced Vertical Break (in)'},
                                           hover_data=['pitch_number', 'release_speed', 'spin_rate'],
                                           opacity=0.7)
                            
                            # Add reference lines
                            fig.add_hline(y=0, line_dash="dash", line_color="gray")
                            fig.add_vline(x=0, line_dash="dash", line_color="gray")
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Release point consistency
                            if 'release_height' in df.columns and 'release_side' in df.columns:
                                df_release = df[df['release_height'].notna() & df['release_side'].notna()]
                                
                                if len(df_release) > 0:
                                    fig2 = px.scatter(df_release, x='release_side', y='release_height',
                                                    title='Release Point Consistency',
                                                    labels={'release_side': 'Release Side (ft)', 
                                                           'release_height': 'Release Height (ft)'},
                                                    hover_data=['pitch_number', 'release_speed'],
                                                    opacity=0.7)
                                    st.plotly_chart(fig2, use_container_width=True)
                        else:
                            st.info("No movement data available for this session")
                    else:
                        st.info("No movement data available for this session")
            else:
                st.warning("No pitch data found for this session")
    
    conn.close()

if __name__ == "__main__":
    main()
