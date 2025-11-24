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

def get_all_players_with_sessions(conn):
    """Get all players who have sessions"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT DISTINCT p.player_id,
               CONCAT(p.first_name, ' ', p.last_name) as player_name,
               p.graduation_year
        FROM players p
        JOIN training_sessions ts ON p.player_id = ts.player_id
        ORDER BY p.last_name, p.first_name
    """)
    return cursor.fetchall()

def get_sessions_by_player(conn, player_id):
    """Get all sessions for a specific player"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ts.session_id, ts.session_date, ts.session_type,
               ts.location,
               CONCAT(c.first_name, ' ', c.last_name) as coach_name,
               COUNT(pd.pitch_id) as pitch_count
        FROM training_sessions ts
        LEFT JOIN coaches c ON ts.coach_id = c.coach_id
        LEFT JOIN pitch_data pd ON ts.session_id = pd.session_id
        WHERE ts.player_id = %s
        GROUP BY ts.session_id, ts.session_date, ts.session_type,
                 ts.location, c.first_name, c.last_name
        ORDER BY ts.session_date DESC, ts.session_id DESC
    """, (player_id,))
    return cursor.fetchall()

def get_session_details(conn, session_id):
    """Get detailed session information"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ts.session_id, ts.player_id, ts.coach_id, ts.session_date,
               ts.session_type, ts.location, ts.session_focus, 
               ts.duration_minutes, ts.coach_notes, ts.data_source_id, ts.created_at,
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
        LEFT JOIN data_sources ds ON ts.data_source_id = ds.source_id
        LEFT JOIN pitch_data pd ON ts.session_id = pd.session_id
        WHERE ts.session_id = %s
        GROUP BY ts.session_id, ts.player_id, ts.coach_id, ts.session_date,
                 ts.session_type, ts.location, ts.session_focus, 
                 ts.duration_minutes, ts.coach_notes, ts.data_source_id, ts.created_at,
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
    results = cursor.fetchall()
    
    # Convert decimal.Decimal to float for pandas compatibility
    for row in results:
        for key, value in row.items():
            if value is not None and type(value).__name__ == 'Decimal':
                row[key] = float(value)
    
    return results

def get_pitch_type_summary(conn, session_id):
    """Get pitch type breakdown for a session with movement stats"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT pitch_type,
               COUNT(*) as count,
               AVG(release_speed) as avg_velocity,
               MIN(release_speed) as min_velocity,
               MAX(release_speed) as max_velocity,
               AVG(horizontal_break) as avg_h_break,
               MIN(horizontal_break) as min_h_break,
               MAX(horizontal_break) as max_h_break,
               AVG(vertical_break) as avg_v_break,
               MIN(vertical_break) as min_v_break,
               MAX(vertical_break) as max_v_break,
               AVG(spin_rate) as avg_spin
        FROM pitch_data
        WHERE session_id = %s AND pitch_type IS NOT NULL
        GROUP BY pitch_type
        ORDER BY count DESC
    """, (session_id,))
    results = cursor.fetchall()
    
    # Convert decimals to float
    for row in results:
        for key, value in row.items():
            if value is not None and type(value).__name__ == 'Decimal':
                row[key] = float(value)
    
    return results

def create_combined_polar_chart(pitches_df, selected_pitch_types, throws_hand):
    """Create combined arm slot and spin direction polar chart for multiple pitches"""
    # Filter by selected pitch types
    if selected_pitch_types and 'All' not in selected_pitch_types:
        pitches_df = pitches_df[pitches_df['pitch_type'].isin(selected_pitch_types)]
    
    if len(pitches_df) == 0:
        return None
    
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

    # Determine arm side and glove side labels based on throwing hand
    if throws_hand == 'R':
        # Right-handed pitcher: arm on third base side (3:00), glove on first base side (9:00)
        right_side_label = "3:00 (Arm Side)"
        left_side_label = "9:00 (Glove Side)"
    else:
        # Left-handed pitcher: arm on first base side (9:00), glove on third base side (3:00)
        left_side_label = "9:00 (Arm Side)"
        right_side_label = "3:00 (Glove Side)"
    
    # Plot arm slots (blue) with enhanced hover
    if 'arm_slot' in pitches_df.columns:
        arm_slot_data = pitches_df[pitches_df['arm_slot'].notna()].copy()
        if len(arm_slot_data) > 0:
            # Prepare hover text for each pitch
            hover_texts = []
            for _, pitch in arm_slot_data.iterrows():
                arm_angle = float(pitch['arm_slot'])
                arm_hours = int(arm_angle / 30)
                arm_minutes = int((arm_angle % 30) * 2)
                arm_time_str = f"{arm_hours}:{arm_minutes:02d}"
                
                velocity_str = f"{pitch.get('release_speed', 'N/A'):.1f} mph" if pd.notna(pitch.get('release_speed')) else "N/A"
                horz_break_str = f"{pitch.get('horizontal_break', 'N/A'):.1f} in" if pd.notna(pitch.get('horizontal_break')) else "N/A"
                vert_break_str = f"{pitch.get('vertical_break', 'N/A'):.1f} in" if pd.notna(pitch.get('vertical_break')) else "N/A"
                
                hover_text = (
                    f"<b>Arm Slot: {arm_time_str}</b><br>"
                    f"Angle: {arm_angle:.0f}°<br>"
                    f"Velocity: {velocity_str}<br>"
                    f"H Break: {horz_break_str}<br>"
                    f"V Break: {vert_break_str}"
                )
                hover_texts.append(hover_text)
            
            fig.add_trace(go.Scatterpolar(
                r=[0.9] * len(arm_slot_data),
                theta=arm_slot_data['arm_slot'].astype(float).tolist(),
                mode='markers',
                marker=dict(size=8, color='blue', opacity=0.6),
                name='Arm Slot',
                showlegend=True,
                hovertemplate='%{hovertext}<extra></extra>',
                hovertext=hover_texts
            ))
    
    # Plot spin directions (red) with enhanced hover
    if 'spin_axis' in pitches_df.columns:
        spin_data = pitches_df[pitches_df['spin_axis'].notna()].copy()
        if len(spin_data) > 0:
            # Prepare hover text for each pitch
            hover_texts = []
            for _, pitch in spin_data.iterrows():
                spin_angle = float(pitch['spin_axis'])
                spin_hours = int(spin_angle / 30)
                spin_minutes = int((spin_angle % 30) * 2)
                spin_time_str = f"{spin_hours}:{spin_minutes:02d}"
                
                velocity_str = f"{pitch.get('release_speed', 'N/A'):.1f} mph" if pd.notna(pitch.get('release_speed')) else "N/A"
                horz_break_str = f"{pitch.get('horizontal_break', 'N/A'):.1f} in" if pd.notna(pitch.get('horizontal_break')) else "N/A"
                vert_break_str = f"{pitch.get('vertical_break', 'N/A'):.1f} in" if pd.notna(pitch.get('vertical_break')) else "N/A"
                
                hover_text = (
                    f"<b>Spin Direction: {spin_time_str}</b><br>"
                    f"Angle: {spin_angle:.0f}°<br>"
                    f"Velocity: {velocity_str}<br>"
                    f"H Break: {horz_break_str}<br>"
                    f"V Break: {vert_break_str}"
                )
                hover_texts.append(hover_text)
            
            fig.add_trace(go.Scatterpolar(
                r=[1.0] * len(spin_data),
                theta=spin_data['spin_axis'].astype(float).tolist(),
                mode='markers',
                marker=dict(size=8, color='red', opacity=0.6),
                name='Spin Direction',
                showlegend=True,
                hovertemplate='%{hovertext}<extra></extra>',
                hovertext=hover_texts
            ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=False, range=[0, 1.1]),
            angularaxis=dict(
                direction='clockwise',
                rotation=90,
                ticktext=['12:00 (OTT)', right_side_label, '6:00 (Sub)', left_side_label],
                tickvals=[0, 90, 180, 270]
            )
        ),
        showlegend=True,
        height=500,
        title="Release Mechanics (Pitcher's View)"
    )
    
    return fig

def create_spin_rate_chart(pitches_df, selected_pitch_types):
    """Create spin rate progression chart with pitch type filtering"""
    # Filter by selected pitch types
    if selected_pitch_types and 'All' not in selected_pitch_types:
        pitches_df = pitches_df[pitches_df['pitch_type'].isin(selected_pitch_types)]
    
    if len(pitches_df) == 0 or 'spin_rate' not in pitches_df.columns:
        return None
    
    pitches_df = pitches_df.dropna(subset=['spin_rate'])
    
    if len(pitches_df) == 0:
        return None
    
    # Add pitch number in session
    pitches_df['pitch_num'] = range(1, len(pitches_df) + 1)
    
    fig = px.line(pitches_df, x='pitch_num', y='spin_rate', 
                  title='Spin Rate Progression',
                  labels={'pitch_num': 'Pitch Number in Session', 'spin_rate': 'Spin Rate (rpm)'},
                  markers=True)
    
    # Color by pitch type if available
    if 'pitch_type' in pitches_df.columns:
        fig = px.line(pitches_df, x='pitch_num', y='spin_rate', color='pitch_type',
                      title='Spin Rate Progression by Pitch Type',
                      labels={'pitch_num': 'Pitch Number in Session', 'spin_rate': 'Spin Rate (rpm)'},
                      markers=True)
    
    fig.update_layout(height=400)
    return fig


def main():
    st.title("📋 Session Detail")
    
    conn = get_db_connection()
    if not conn:
        st.error("Could not connect to database")
        return
    
    # Check if a session was selected from another page
    preselected_session_id = st.session_state.get('selected_session_id')
    preselected_player_id = st.session_state.get('selected_player_id')
    
    # Get all players with sessions
    players = get_all_players_with_sessions(conn)
    
    if not players:
        st.warning("No players with training sessions found in database")
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
        key="session_detail_player_selector"
    )
    
    player_id = player_options[selected_player]
    
    # Step 2: Session selection (filtered by selected player)
    sessions = get_sessions_by_player(conn, player_id)
    
    if not sessions:
        st.warning(f"No training sessions found for {selected_player}")
        conn.close()
        return
    
    # Create session options for dropdown
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
        key="session_detail_session_selector"
    )
    
    session_id = session_options[selected_session]
    
    # Clear the preselected values from state after using them
    if 'selected_session_id' in st.session_state:
        del st.session_state['selected_session_id']
    if 'selected_player_id' in st.session_state:
        del st.session_state['selected_player_id']
    
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
                        if pitch['vertical_break']:
                            movement.append(f"V: {pitch['vertical_break']:.1f}\"")
                        if pitch['release_height']:
                            movement.append(f"Height: {pitch['release_height']:.2f}'")
                        st.write(" | ".join(movement) if movement else "No movement data")
                    
                    with col5:
                        if st.button("View", key=f"pitch_{pitch['pitch_id']}", width='stretch'):
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
                    'V Break': f"{pitch['vertical_break']:.1f}\"" if pitch['vertical_break'] else 'N/A',
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
            
            # Pitch Type Breakdown
            pitch_type_summary = get_pitch_type_summary(conn, session_id)
            if pitch_type_summary:
                st.subheader("📊 Pitch Type Breakdown")
                
                # Create summary table
                summary_data = []
                for pt in pitch_type_summary:
                    summary_data.append({
                        'Pitch Type': pt['pitch_type'],
                        'Count': pt['count'],
                        'Velocity': f"{pt['min_velocity']:.1f} / {pt['avg_velocity']:.1f} / {pt['max_velocity']:.1f}" if pt['avg_velocity'] else 'N/A',
                        'H Break (in)': f"{pt['min_h_break']:.1f} / {pt['avg_h_break']:.1f} / {pt['max_h_break']:.1f}" if pt['avg_h_break'] else 'N/A',
                        'V Break (in)': f"{pt['min_v_break']:.1f} / {pt['avg_v_break']:.1f} / {pt['max_v_break']:.1f}" if pt['avg_v_break'] else 'N/A',
                        'Avg Spin': f"{pt['avg_spin']:.0f}" if pt['avg_spin'] else 'N/A'
                    })
                
                summary_df = pd.DataFrame(summary_data)
                st.dataframe(summary_df, width='stretch', hide_index=True)
                st.caption("📌 Format: Min / Avg / Max")
                
                st.markdown("---")

            # Pitch type filter for charts
            st.subheader("Analytics")
            if 'pitch_type' in df.columns:
                pitch_types = ['All'] + sorted(df['pitch_type'].dropna().unique().tolist())
                selected_pitch_types = st.multiselect(
                    "Filter by Pitch Type",
                    options=pitch_types,
                    default=['All'],
                    key='pitch_type_filter'
                )
            else:
                selected_pitch_types = ['All']
            
            # Velocity stats
            if 'release_speed' in df.columns and df['release_speed'].notna().any():
                st.subheader("📈 Velocity Analysis")
                
                # Check if we have pitch types
                has_pitch_types = 'pitch_type' in df.columns and df['pitch_type'].notna().any()
                
                if has_pitch_types:
                    pitch_types = df[df['pitch_type'].notna()]['pitch_type'].unique()
                    
                    # Add pitch type filter
                    selected_types = st.multiselect(
                        "Filter by pitch type (leave empty for all)",
                        options=list(pitch_types),
                        default=list(pitch_types),
                        key="session_velocity_filter"
                    )
                    
                    if selected_types:
                        df_filtered = df[df['pitch_type'].isin(selected_types)]
                    else:
                        df_filtered = df
                else:
                    df_filtered = df
                    selected_types = None
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Average", f"{df_filtered['release_speed'].mean():.1f} mph")
                with col2:
                    st.metric("Maximum", f"{df_filtered['release_speed'].max():.1f} mph")
                with col3:
                    st.metric("Minimum", f"{df_filtered['release_speed'].min():.1f} mph")
                with col4:
                    st.metric("Std Dev", f"{df_filtered['release_speed'].std():.2f} mph")
                
                # Velocity progression chart
                st.subheader("Velocity Throughout Session")
                
                if has_pitch_types and selected_types:
                    fig = px.scatter(df_filtered, x='pitch_number', y='release_speed',
                                color='pitch_type',
                                title='Velocity by Pitch Number (Color = Pitch Type)',
                                labels={'pitch_number': 'Pitch Number', 'release_speed': 'Velocity (mph)', 'pitch_type': 'Pitch Type'})
                    fig.add_hline(y=df_filtered['release_speed'].mean(), line_dash="dash", 
                                line_color="gray", annotation_text="Overall Average")
                else:
                    fig = px.line(df_filtered, x='pitch_number', y='release_speed',
                                title='Velocity by Pitch Number',
                                labels={'pitch_number': 'Pitch Number', 'release_speed': 'Velocity (mph)'})
                fig.add_hline(y=df['release_speed'].mean(), line_dash="dash", 
                            line_color="red", annotation_text="Average")
                st.plotly_chart(fig, width='stretch')
                
                # Combined polar chart (replaces velocity distribution)
                st.subheader("Release Mechanics")
                fig = create_combined_polar_chart(df, selected_pitch_types, session['throws_hand'])
                if fig:
                    st.plotly_chart(fig, width='stretch')
                else:
                    st.info("Release mechanics data not available for selected pitch types")
                    
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
                st.plotly_chart(fig, width='stretch')
            
            # Movement analysis
            if 'horizontal_break' in df.columns and 'vertical_break' in df.columns:
                df_movement = df[df['horizontal_break'].notna() & df['vertical_break'].notna()]
                if len(df_movement) > 0:
                    st.subheader("⚾ Movement Analysis")
                    
                    # Check for pitch types
                    has_pitch_types = 'pitch_type' in df_movement.columns and df_movement['pitch_type'].notna().any()
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Horizontal Break**")
                        st.metric("Average", f"{df_movement['horizontal_break'].mean():.2f}\"")
                        st.metric("Range", f"{df_movement['horizontal_break'].min():.2f}\" to {df_movement['horizontal_break'].max():.2f}\"")
                    
                    with col2:
                        st.write("**Vertical Break**")
                        st.metric("Average", f"{df_movement['vertical_break'].mean():.2f}\"")
                        st.metric("Range", f"{df_movement['vertical_break'].min():.2f}\" to {df_movement['vertical_break'].max():.2f}\"")
                    
                    # Movement chart
                    st.subheader("Pitch Movement Profile")
                    
                    if has_pitch_types:
                        fig = px.scatter(df_movement, x='horizontal_break', y='vertical_break',
                                       color='pitch_type',
                                       title='Movement Chart by Pitch Type',
                                       labels={'horizontal_break': 'Horizontal Break (in)', 
                                              'vertical_break': 'Vertical Break (in)',
                                              'pitch_type': 'Pitch Type'},
                                       hover_data=['pitch_number', 'release_speed'],
                                       opacity=0.7)
                    else:
                        fig = px.scatter(df_movement, x='horizontal_break', y='vertical_break',
                                       title='Movement Chart',
                                       labels={'horizontal_break': 'Horizontal Break (in)', 
                                              'vertical_break': 'Vertical Break (in)'},
                                       hover_data=['pitch_number', 'release_speed'],
                                       opacity=0.7)
                    
                    fig.add_hline(y=0, line_dash="dash", line_color="gray")
                    fig.add_vline(x=0, line_dash="dash", line_color="gray")
                    st.plotly_chart(fig, width='stretch')
        else:
            st.info("No pitch data available for analytics")
    
    conn.close()

if __name__ == "__main__":
    main()
