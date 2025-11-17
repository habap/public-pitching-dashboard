"""
Pitch Detail Page
View detailed information about a specific pitch
"""

import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import plotly.graph_objects as go

# Page configuration
st.set_page_config(
    page_title="Pitch Detail",
    page_icon="⚾",
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

def get_pitch_details(conn, pitch_id):
    """Get detailed pitch information"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT pd.*,
               ts.session_id, ts.session_date, ts.session_type, ts.location,
               CONCAT(p.first_name, ' ', p.last_name) as player_name,
               p.player_id, p.graduation_year, p.throws_hand,
               CONCAT(c.first_name, ' ', c.last_name) as coach_name,
               c.coach_id,
               ds.source_name
        FROM pitch_data pd
        JOIN training_sessions ts ON pd.session_id = ts.session_id
        JOIN players p ON ts.player_id = p.player_id
        LEFT JOIN coaches c ON ts.coach_id = c.coach_id
        LEFT JOIN data_sources ds ON ts.data_source_id = ds.source_id
        WHERE pd.pitch_id = %s
    """, (pitch_id,))
    result = cursor.fetchone()
    
    # Convert decimal.Decimal to float for compatibility
    if result:
        for key, value in result.items():
            if value is not None and type(value).__name__ == 'Decimal':
                result[key] = float(value)
    
    return result

def get_session_pitches(conn, session_id):
    """Get all pitches in a session for numbering"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT pitch_id, pitch_timestamp
        FROM pitch_data
        WHERE session_id = %s
        ORDER BY pitch_timestamp
    """, (session_id,))
    return cursor.fetchall()

def get_pitcher_pitches(conn, player_id, pitch_timestamp):
    """Get all pitches for a pitcher up to this pitch for numbering"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT pd.pitch_id, pd.pitch_timestamp
        FROM pitch_data pd
        JOIN training_sessions ts ON pd.session_id = ts.session_id
        WHERE ts.player_id = %s AND pd.pitch_timestamp <= %s
        ORDER BY pd.pitch_timestamp
    """, (player_id, pitch_timestamp))
    return cursor.fetchall()

def create_combined_polar_chart(pitch):
    """Create combined arm slot and spin direction polar chart"""
    fig = go.Figure()
    
    # Create circle background
    theta = list(range(0, 361, 5))
    r = [1] * len(theta)
    
    fig.add_trace(go.Scatterpolar(
        r=r,
        theta=theta,
        mode='lines',
        line=dict(color='lightgray', width=2),
        showlegend=False,
        hoverinfo='skip'
    ))
    
    # Add arm slot line (if available)
    if pitch.get('arm_slot') is not None:
        arm_angle = float(pitch['arm_slot'])
        fig.add_trace(go.Scatterpolar(
            r=[0, 1],
            theta=[arm_angle, arm_angle],
            mode='lines+markers',
            line=dict(color='blue', width=4),
            marker=dict(size=[0, 12], color='blue'),
            name='Arm Slot',
            showlegend=True
        ))
    
    # Add spin direction line (if available)
    if pitch.get('spin_axis') is not None:
        spin_angle = float(pitch['spin_axis'])
        fig.add_trace(go.Scatterpolar(
            r=[0, 1],
            theta=[spin_angle, spin_angle],
            mode='lines+markers',
            line=dict(color='red', width=4),
            marker=dict(size=[0, 12], color='red'),
            name='Spin Direction',
            showlegend=True
        ))
    
    # Determine arm side and glove side labels based on throwing hand
    if pitch.get('throws_hand'):
        if pitch['throws_hand'] == 'R':
            arm_side = "9:00 (Arm Side)"
            glove_side = "3:00 (Glove Side)"
        else:
            arm_side = "3:00 (Arm Side)"
            glove_side = "9:00 (Glove Side)"
    else:
        arm_side = "9:00"
        glove_side = "3:00"
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=False, range=[0, 1]),
            angularaxis=dict(
                direction='clockwise',
                rotation=90,
                ticktext=['12:00 (OTT)', glove_side, '6:00 (Sub)', arm_side],
                tickvals=[0, 90, 180, 270]
            )
        ),
        showlegend=True,
        height=500,
        title="Release Mechanics (Pitcher's View)"
    )
    
    return fig
    
def get_all_pitches_dropdown(conn):
    """Get all pitches for dropdown selection"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT pd.pitch_id, pd.pitch_number,
               ts.session_id, ts.session_date,
               CONCAT(p.first_name, ' ', p.last_name) as player_name,
               pd.release_speed
        FROM pitch_data pd
        JOIN training_sessions ts ON pd.session_id = ts.session_id
        JOIN players p ON ts.player_id = p.player_id
        ORDER BY ts.session_date DESC, pd.pitch_number
        LIMIT 500
    """)
    results = cursor.fetchall()
    
    # Convert decimals to float
    for row in results:
        for key, value in row.items():
            if value is not None and type(value).__name__ == 'Decimal':
                row[key] = float(value)
    
    return results

def get_all_players_with_pitches(conn):
    """Get all players who have pitch data"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT DISTINCT p.player_id,
               CONCAT(p.first_name, ' ', p.last_name) as player_name,
               p.graduation_year
        FROM players p
        JOIN training_sessions ts ON p.player_id = ts.player_id
        JOIN pitch_data pd ON ts.session_id = pd.session_id
        ORDER BY p.last_name, p.first_name
    """)
    return cursor.fetchall()

def get_sessions_with_pitches_by_player(conn, player_id):
    """Get all sessions with pitch data for a specific player"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT DISTINCT ts.session_id, ts.session_date, ts.session_type,
               COUNT(pd.pitch_id) as pitch_count
        FROM training_sessions ts
        JOIN pitch_data pd ON ts.session_id = pd.session_id
        WHERE ts.player_id = %s
        GROUP BY ts.session_id, ts.session_date, ts.session_type
        ORDER BY ts.session_date DESC
    """, (player_id,))
    return cursor.fetchall()

def get_pitches_by_session(conn, session_id):
    """Get all pitches for a specific session"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT pd.pitch_id, pd.pitch_number, pd.release_speed
        FROM pitch_data pd
        WHERE pd.session_id = %s
        ORDER BY pd.pitch_number
    """, (session_id,))
    results = cursor.fetchall()
    
    # Convert decimals to float
    for row in results:
        for key, value in row.items():
            if value is not None and type(value).__name__ == 'Decimal':
                row[key] = float(value)
    
    return results

def main():
    st.title("⚾ Pitch Detail")
    
    conn = get_db_connection()
    if not conn:
        st.error("Could not connect to database")
        return
    
    # Check if selections were made from another page
    preselected_pitch_id = st.session_state.get('selected_pitch_id')
    preselected_session_id = st.session_state.get('selected_session_id')
    preselected_player_id = st.session_state.get('session_player_id')
    
    # Get all players with pitch data
    players = get_all_players_with_pitches(conn)
    
    if not players:
        st.warning("No players with pitch data found in database")
        conn.close()
        return
    
    # Step 1: Player selection
    player_options = {
        f"{p['player_name']} ({p['graduation_year']})": p['player_id'] 
        for p in players
    }
    
    # If there's a preselected player, use it as default
    if preselected_player_id and preselected_player_id in player_options.values():
        default_player_index = list(player_options.values()).index(preselected_player_id)
    else:
        default_player_index = 0
    
    selected_player = st.selectbox(
        "👤 Select Player",
        list(player_options.keys()),
        index=default_player_index,
        key="pitch_detail_player_selector"
    )
    
    player_id = player_options[selected_player]
    
    # Step 2: Session selection (filtered by selected player)
    sessions = get_sessions_with_pitches_by_player(conn, player_id)
    
    if not sessions:
        st.warning(f"No sessions with pitch data found for {selected_player}")
        conn.close()
        return
    
    # Create session options
    session_options = {
        f"{s['session_date'].strftime('%m/%d/%Y')} - {s['session_type']} ({s['pitch_count']} pitches)": s['session_id']
        for s in sessions
    }
    
    # If there's a preselected session, use it as default
    if preselected_session_id and preselected_session_id in session_options.values():
        default_session_index = list(session_options.values()).index(preselected_session_id)
    else:
        default_session_index = 0
    
    selected_session = st.selectbox(
        "📅 Select Session",
        list(session_options.keys()),
        index=default_session_index,
        key="pitch_detail_session_selector"
    )
    
    session_id = session_options[selected_session]
    
    # Step 3: Pitch selection (filtered by selected session)
    pitches = get_pitches_by_session(conn, session_id)
    
    if not pitches:
        st.warning("No pitches found for this session")
        conn.close()
        return
    
    # Create pitch options
    pitch_options = {}
    for p in pitches:
        if p.get('release_speed'):
            label = f"Pitch #{p['pitch_number']} - {p['release_speed']:.1f} mph"
        else:
            label = f"Pitch #{p['pitch_number']}"
        pitch_options[label] = p['pitch_id']
    
    # If there's a preselected pitch, use it as default
    if preselected_pitch_id and preselected_pitch_id in pitch_options.values():
        default_pitch_index = list(pitch_options.values()).index(preselected_pitch_id)
    else:
        default_pitch_index = 0
    
    selected_pitch = st.selectbox(
        "⚾ Select Pitch",
        list(pitch_options.keys()),
        index=default_pitch_index,
        key="pitch_detail_pitch_selector"
    )
    
    pitch_id = pitch_options[selected_pitch]
    
    # Clear preselected values from state after using them
    if 'selected_pitch_id' in st.session_state:
        del st.session_state['selected_pitch_id']
    if 'selected_session_id' in st.session_state:
        del st.session_state['selected_session_id']
    if 'session_player_id' in st.session_state:
        del st.session_state['session_player_id']
    if 'session_player_name' in st.session_state:
        del st.session_state['session_player_name']
    
    # Get pitch details
    pitch = get_pitch_details(conn, pitch_id)
    
    if not pitch:
        st.error("Pitch not found")
        conn.close()
        return
    
    # Navigation breadcrumb
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 2, 2])
    
    with col1:
        if st.button(f"← Back to {pitch['player_name']}'s Profile", key="back_to_player"):
            st.session_state['selected_player_id'] = pitch['player_id']
            st.switch_page("pages/1_Player_Profile.py")
    
    with col2:
        if st.button(f"← Back to Session", key="back_to_session"):
            st.session_state['selected_session_id'] = pitch['session_id']
            st.session_state['selected_player_id'] = pitch['player_id']
            st.switch_page("pages/2_Session_Detail.py")
    
    # Clear session state after displaying navigation
    if 'selected_pitch_id' in st.session_state:
        del st.session_state['selected_pitch_id']
    if 'selected_session_id' in st.session_state and not st.session_state.get('keep_session_id'):
        # Only clear if we're not using it for navigation
        pass
    if 'session_player_name' in st.session_state:
        del st.session_state['session_player_name']
    if 'session_player_id' in st.session_state:
        del st.session_state['session_player_id']
    
    st.markdown("---")
    
    # Get pitch numbers (context-aware)
    session_pitches = get_session_pitches(conn, pitch['session_id'])
    session_pitch_num = next((i+1 for i, p in enumerate(session_pitches) if p['pitch_id'] == pitch_id), None)
    
    pitcher_pitches = get_pitcher_pitches(conn, pitch['player_id'], pitch['pitch_timestamp'])
    pitcher_pitch_num = next((i+1 for i, p in enumerate(pitcher_pitches) if p['pitch_id'] == pitch_id), None)
    
    # Pitch Header with context numbering
    st.markdown(f"### {pitch['player_name']} - {pitch.get('pitch_type', 'Unknown')} Pitch")
    st.markdown(f"**Session:** {pitch['session_date'].strftime('%m/%d/%Y')} | **Pitch #{session_pitch_num}** in session | **Pitch #{pitcher_pitch_num}** for pitcher")
    st.markdown("---")

    # Pitch Header
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Pitch Information")
        if pitch.get('pitch_type'):
            st.metric("Pitch Type", pitch['pitch_type'])
        st.write(f"**Session Date:** {pitch['session_date'].strftime('%m/%d/%Y')}")
        st.write(f"**Session Type:** {pitch['session_type']}")
        if pitch['location']:
            st.write(f"**Location:** {pitch['location']}")
    
    with col2:
        st.subheader("Player")
        st.metric("Name", pitch['player_name'])
        if pitch['throws_hand']:
            st.write(f"**Throws:** {pitch['throws_hand']}")
        if pitch['graduation_year']:
            st.write(f"**Class of:** {pitch['graduation_year']}")
    
    with col3:
        st.subheader("Session Info")
        st.write(f"**Session ID:** {pitch['session_id']}")
        if pitch['coach_name']:
            st.write(f"**Coach:** {pitch['coach_name']}")
        if pitch['source_name']:
            st.write(f"**Data Source:** {pitch['source_name']}")
    
    st.markdown("---")
    
    # Pitch Metrics in organized sections
    st.header("Pitch Metrics")
    
    # Velocity Section
    st.subheader("🏃 Velocity & Speed")
    col1, col2 = st.columns(2)
    
    with col1:
        if pitch['release_speed']:
            st.metric("Release Speed", f"{pitch['release_speed']:.1f} mph", 
                     help="Velocity of the ball as it leaves the pitcher's hand")
        else:
            st.metric("Release Speed", "N/A")
    
    with col2:
        if pitch.get('perceived_velocity'):
            st.metric("Perceived Velocity", f"{pitch['perceived_velocity']:.1f} mph",
                     help="Effective velocity from the batter's perspective")
        else:
            st.metric("Perceived Velocity", "N/A")
    
    st.divider()
    
    # Spin Section
    st.subheader("🌀 Spin Characteristics")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if pitch['spin_rate']:
            st.metric("Spin Rate", f"{pitch['spin_rate']:.0f} rpm",
                     help="Rotational speed of the ball")
        else:
            st.metric("Spin Rate", "N/A")
    
    with col2:
        if pitch['spin_axis']:
            st.metric("Spin Axis", f"{pitch['spin_axis']:.0f}°",
                     help="Direction of the spin axis (0-360°)")
        else:
            st.metric("Spin Axis", "N/A")
    
    with col3:
        if pitch['spin_efficiency']:
            st.metric("Spin Efficiency", f"{pitch['spin_efficiency']:.1f}%",
                     help="Percentage of spin contributing to movement")
        else:
            st.metric("Spin Efficiency", "N/A")
    
# Combined polar chart for arm slot and spin direction
    if pitch['spin_axis'] or pitch.get('arm_slot'):
        st.subheader("Release Mechanics")
        fig = create_combined_polar_chart(pitch)
        st.plotly_chart(fig, width='strech')
        
        # Add descriptive text
        descriptions = []
        if pitch.get('arm_slot') is not None:
            arm_angle = float(pitch['arm_slot'])
            if 345 <= arm_angle or arm_angle <= 15:
                slot_type = "Over-the-top"
            elif 15 < arm_angle <= 60:
                slot_type = "High 3/4"
            elif 60 < arm_angle <= 120:
                slot_type = "3/4"
            elif 120 < arm_angle <= 150:
                slot_type = "Low 3/4"
            elif 150 < arm_angle <= 200:
                slot_type = "Sidearm"
            else:
                slot_type = "Submarine"
            descriptions.append(f"📐 **Arm Slot:** {slot_type} ({arm_angle:.0f}°)")
        
        if pitch.get('spin_axis') is not None:
            spin_angle = float(pitch['spin_axis'])
            descriptions.append(f"🔴 **Spin Direction:** {spin_angle:.0f}°")
        
        for desc in descriptions:
            st.markdown(desc)
            
    st.divider()
    
    # Movement Section
    st.subheader("↔️ Pitch Movement")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if pitch['horizontal_break']:
            st.metric("Horizontal Break", f"{pitch['horizontal_break']:.1f}\"",
                     help="Horizontal movement of the pitch (positive = arm side)")
        else:
            st.metric("Horizontal Break", "N/A")
    
    with col2:
        if pitch['induced_vertical_break']:
            st.metric("Induced Vertical Break", f"{pitch['induced_vertical_break']:.1f}\"",
                     help="Vertical movement relative to gravity")
        else:
            st.metric("Induced Vertical Break", "N/A")
    
    with col3:
        if pitch['vertical_break']:
            st.metric("Vertical Break", f"{pitch['vertical_break']:.1f}\"",
                     help="Total vertical movement including gravity")
        else:
            st.metric("Vertical Break", "N/A")
    
    # Movement visualization
    if pitch['horizontal_break'] is not None and pitch['induced_vertical_break'] is not None:
        st.subheader("Movement Plot (Catcher's View)")
        fig = go.Figure()
        
        # Add the pitch as a point
        fig.add_trace(go.Scatter(
            x=[pitch['horizontal_break']],
            y=[pitch['induced_vertical_break']],
            mode='markers',
            marker=dict(size=20, color='red', symbol='circle'),
            name='This Pitch',
            showlegend=True
        ))
        
        # Add reference lines
        fig.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="No vertical break")
        fig.add_vline(x=0, line_dash="dash", line_color="gray", annotation_text="No horizontal break")
        
        fig.update_layout(
            xaxis_title="Horizontal Break (inches)",
            yaxis_title="Induced Vertical Break (inches)",
            height=500,
            showlegend=True
        )
        
        # Set axis ranges for better visualization
        max_val = max(abs(pitch['horizontal_break']), abs(pitch['induced_vertical_break'])) + 5
        fig.update_xaxes(range=[-max_val, max_val])
        fig.update_yaxes(range=[-max_val, max_val])
        
        st.plotly_chart(fig, width='strech')
    
    st.divider()
    
    # Release Point Section
    st.subheader("📍 Release Point")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if pitch['release_height']:
            st.metric("Release Height", f"{pitch['release_height']:.2f}'",
                     help="Height of ball release above ground")
        else:
            st.metric("Release Height", "N/A")
    
    with col2:
        if pitch['release_side']:
            st.metric("Release Side", f"{pitch['release_side']:.2f}'",
                     help="Horizontal position at release (positive = toward 1B)")
        else:
            st.metric("Release Side", "N/A")
    
    with col3:
        if pitch['release_extension']:
            st.metric("Extension", f"{pitch['release_extension']:.2f}'",
                     help="Distance from rubber to release point")
        else:
            st.metric("Extension", "N/A")
    
    st.divider()
    
    # Trajectory Section
    st.subheader("📐 Trajectory")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if pitch['vert_approach_angle']:
            st.metric("Vertical Approach Angle", f"{pitch['vert_approach_angle']:.1f}°",
                     help="Angle of ball's descent as it crosses plate")
        else:
            st.metric("Vertical Approach Angle", "N/A")
    
    with col2:
        if pitch['horz_approach_angle']:
            st.metric("Horizontal Approach Angle", f"{pitch['horz_approach_angle']:.1f}°",
                     help="Horizontal angle as pitch crosses plate")
        else:
            st.metric("Horizontal Approach Angle", "N/A")
    
    with col3:
        if pitch['plate_crossing_time']:
            st.metric("Time to Plate", f"{pitch['plate_crossing_time']:.3f} sec",
                     help="Time from release to home plate")
        else:
            st.metric("Time to Plate", "N/A")
    
    st.divider()
    
    # Strike Zone Section (if available)
    if pitch['plate_location_x'] is not None or pitch['plate_location_z'] is not None:
        st.subheader("🎯 Strike Zone Location")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            if pitch['plate_location_x'] is not None and pitch['plate_location_z'] is not None:
                # Create strike zone plot
                fig = go.Figure()
                
                # Draw strike zone (approximate)
                zone_width = 17/12  # 17 inches in feet
                zone_height_top = 3.5
                zone_height_bottom = 1.5
                
                # Strike zone rectangle
                fig.add_shape(
                    type="rect",
                    x0=-zone_width/2, x1=zone_width/2,
                    y0=zone_height_bottom, y1=zone_height_top,
                    line=dict(color="black", width=2),
                    fillcolor="lightblue",
                    opacity=0.2
                )
                
                # Add the pitch location
                fig.add_trace(go.Scatter(
                    x=[pitch['plate_location_x']],
                    y=[pitch['plate_location_z']],
                    mode='markers',
                    marker=dict(size=15, color='red', symbol='circle'),
                    name='Pitch Location',
                    showlegend=True
                ))
                
                fig.update_layout(
                    xaxis_title="Horizontal Position (feet from plate center)",
                    yaxis_title="Vertical Position (feet above ground)",
                    height=500,
                    showlegend=True,
                    title="Pitch Location at Home Plate (Catcher's View)"
                )
                
                fig.update_xaxes(range=[-2, 2])
                fig.update_yaxes(range=[0, 5])
                
                st.plotly_chart(fig, width='strech')
        
        with col2:
            st.metric("Plate X", f"{pitch['plate_location_x']:.2f}'" if pitch['plate_location_x'] else "N/A",
                     help="Horizontal position at plate (0 = center)")
            st.metric("Plate Z", f"{pitch['plate_location_z']:.2f}'" if pitch['plate_location_z'] else "N/A",
                     help="Vertical position at plate")
    
    st.divider()
    
    # Additional Metrics
    st.subheader("📊 Additional Metrics")
    col1, col2 = st.columns(2)
    
    with col1:
        if pitch.get('acceleration_x'):
            st.metric("X Acceleration", f"{pitch['acceleration_x']:.1f} ft/s²")
        if pitch.get('acceleration_y'):
            st.metric("Y Acceleration", f"{pitch['acceleration_y']:.1f} ft/s²")
        if pitch.get('acceleration_z'):
            st.metric("Z Acceleration", f"{pitch['acceleration_z']:.1f} ft/s²")
    
    with col2:
        if pitch.get('max_height'):
            st.metric("Max Height", f"{pitch['max_height']:.2f}'",
                     help="Highest point of pitch trajectory")
        if pitch.get('flight_time'):
            st.metric("Flight Time", f"{pitch['flight_time']:.3f} sec")
    
    # Notes section if available
    if pitch.get('notes'):
        st.divider()
        st.subheader("📝 Notes")
        st.write(pitch['notes'])
    
    conn.close()

if __name__ == "__main__":
    main()
