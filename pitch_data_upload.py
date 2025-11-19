"""
Player Profile Page
View individual player details including sessions, pitches, coaches, and locations
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
    page_title="Player Profile",
    page_icon="👤",
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

def get_all_players(conn):
    """Get all active players"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT player_id, first_name, last_name,
               CONCAT(first_name, ' ', last_name) AS player_name,
               graduation_year, throws_hand, bats_hand, 
               email, phone, parent_email
        FROM players 
        WHERE is_active = TRUE
        ORDER BY last_name, first_name
    """)
    return cursor.fetchall()

def get_player_details(conn, player_id):
    """Get detailed player information"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT p.*,
               COUNT(DISTINCT ts.session_id) as total_sessions,
               COUNT(pd.pitch_id) as total_pitches,
               MIN(ts.session_date) as first_session,
               MAX(ts.session_date) as last_session
        FROM players p
        LEFT JOIN training_sessions ts ON p.player_id = ts.player_id
        LEFT JOIN pitch_data pd ON ts.session_id = pd.session_id
        WHERE p.player_id = %s
        GROUP BY p.player_id
    """, (player_id,))
    return cursor.fetchone()

def get_player_sessions(conn, player_id):
    """Get all sessions for a player"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ts.session_id, ts.session_date, ts.session_type,
               ts.location, ts.session_focus, ts.duration_minutes,
               COUNT(pd.pitch_id) as pitch_count,
               AVG(pd.release_speed) as avg_velocity,
               MAX(pd.release_speed) as max_velocity,
               AVG(pd.spin_rate) as avg_spin,
               ds.source_name,
               CONCAT(c.first_name, ' ', c.last_name) as coach_name,
               c.coach_id
        FROM training_sessions ts
        LEFT JOIN pitch_data pd ON ts.session_id = pd.session_id
        LEFT JOIN data_sources ds ON ts.data_source_id = ds.source_id
        LEFT JOIN coaches c ON ts.coach_id = c.coach_id
        WHERE ts.player_id = %s
        GROUP BY ts.session_id, ts.session_date, ts.session_type,
                 ts.location, ts.session_focus, ts.duration_minutes,
                 ds.source_id, ds.source_name, c.first_name, c.last_name, c.coach_id
        ORDER BY ts.session_date DESC, ts.session_id DESC
    """, (player_id,))
    return cursor.fetchall()

def get_player_pitch_data(conn, player_id, limit=100):
    """Get recent pitch data for a player"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT pd.*, ts.session_date, ts.session_type, ts.session_id,
               ds.source_name
        FROM pitch_data pd
        JOIN training_sessions ts ON pd.session_id = ts.session_id
        JOIN data_sources ds ON ts.data_source_id = ds.source_id
        WHERE ts.player_id = %s
        ORDER BY ts.session_date DESC, pd.pitch_number
        LIMIT %s
    """, (player_id, limit))
    results = cursor.fetchall()
    
    # Convert decimal.Decimal to float for pandas compatibility
    for row in results:
        for key, value in row.items():
            if value is not None and type(value).__name__ == 'Decimal':
                row[key] = float(value)
    
    return results

def get_player_coaches(conn, player_id):
    """Get all coaches who have worked with this player"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT DISTINCT c.coach_id, 
               CONCAT(c.first_name, ' ', c.last_name) as coach_name,
               c.email, c.phone, c.organization,
               COUNT(DISTINCT ts.session_id) as session_count,
               MIN(ts.session_date) as first_session,
               MAX(ts.session_date) as last_session
        FROM coaches c
        JOIN training_sessions ts ON c.coach_id = ts.coach_id
        WHERE ts.player_id = %s
        GROUP BY c.coach_id
        ORDER BY session_count DESC
    """, (player_id,))
    return cursor.fetchall()

def get_player_locations(conn, player_id):
    """Get all locations where player has trained"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ts.location,
               COUNT(DISTINCT ts.session_id) as session_count,
               COUNT(pd.pitch_id) as pitch_count,
               MIN(ts.session_date) as first_visit,
               MAX(ts.session_date) as last_visit
        FROM training_sessions ts
        LEFT JOIN pitch_data pd ON ts.session_id = pd.session_id
        WHERE ts.player_id = %s AND ts.location IS NOT NULL AND ts.location != ''
        GROUP BY ts.location
        ORDER BY session_count DESC
    """, (player_id,))
    return cursor.fetchall()

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
               AVG(induced_vertical_break) as avg_v_break,
               MIN(induced_vertical_break) as min_v_break,
               MAX(induced_vertical_break) as max_v_break,
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

def main():
    st.title("👤 Player Profile")
    
    conn = get_db_connection()
    if not conn:
        st.error("Could not connect to database")
        return
    
    # Player selection
    players = get_all_players(conn)
    
    if not players:
        st.warning("No players found in database")
        conn.close()
        return
    
    # Create alphabetically sorted player options
    player_options = {f"{p['player_name']} ({p['graduation_year']})": p['player_id'] 
                     for p in players}
    sorted_player_names = sorted(player_options.keys())
    
    # Check if a player was pre-selected (from Session or Pitch Detail page)
    selected_player_id = st.session_state.get('selected_player_id')
    
    # Handle clear button trigger
    if 'clear_player_search' in st.session_state and st.session_state.clear_player_search:
        if 'player_profile_search' in st.session_state:
            del st.session_state.player_profile_search
        st.session_state.clear_player_search = False
        st.rerun()
    
    # Type-ahead search box with live filtering using on_change
    col1, col2 = st.columns([4, 1])
    
    with col1:
        # Use on_change to trigger rerun on every keypress
        search_term = st.text_input(
            "🔍 Search for a player",
            placeholder="Type name, graduation year, or any text to filter...",
            key="player_profile_search",
            help="Start typing to see matching players below",
            on_change=lambda: None  # Triggers rerun on each keypress
        )
    
    with col2:
        if search_term:
            if st.button("✖ Clear", key="clear_profile_search_button"):
                st.session_state.clear_player_search = True
                st.rerun()
    
    # Filter players based on search (alphabetically sorted)
    if search_term:
        filtered_names = [name for name in sorted_player_names 
                         if search_term.lower() in name.lower()]
        filtered_options = {name: player_options[name] for name in filtered_names}
        # Show match count
        if filtered_options:
            st.info(f"✓ Found {len(filtered_options)} player(s) matching '{search_term}'")
        else:
            st.warning(f"No players found matching '{search_term}'. Try a different search.")
    else:
        filtered_options = {name: player_options[name] for name in sorted_player_names}
        st.caption(f"💡 {len(filtered_options)} total players available - type above to filter")
    
    # Show filtered results in selectbox (already sorted alphabetically)
    if not filtered_options:
        st.error("No players match your search")
        conn.close()
        return
    
    # If there's a pre-selected player and it's in filtered list, use it as default
    default_index = 0
    filtered_names_list = list(filtered_options.keys())
    if selected_player_id and selected_player_id in filtered_options.values():
        # Find the index of the selected player in filtered list
        for idx, name in enumerate(filtered_names_list):
            if filtered_options[name] == selected_player_id:
                default_index = idx
                break
    
    selected_player = st.selectbox(
        "Select a player", 
        filtered_names_list,
        index=default_index,
        key="player_profile_select"
    )
    player_id = filtered_options[selected_player]
    
    # Clear the selected player from state after using it
    if 'selected_player_id' in st.session_state:
        del st.session_state['selected_player_id']
    
    # Get player details
    player = get_player_details(conn, player_id)
    
    if not player:
        st.error("Player not found")
        conn.close()
        return
    
    # Player Header
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Name", f"{player['first_name']} {player['last_name']}")
        st.metric("Throws", player['throws_hand'] or "Unknown")
    
    with col2:
        st.metric("Graduation Year", player['graduation_year'] or "N/A")
        st.metric("Bats", player['bats_hand'] or "Unknown")
    
    with col3:
        st.metric("Total Sessions", player['total_sessions'])
        st.metric("Total Pitches", player['total_pitches'])
    
    with col4:
        if player['first_session']:
            st.metric("First Session", player['first_session'].strftime('%m/%d/%Y'))
        if player['last_session']:
            st.metric("Last Session", player['last_session'].strftime('%m/%d/%Y'))
    
    # Contact Information
    with st.expander("📧 Contact Information"):
        if player.get('email'):
            st.write(f"**Email:** {player['email']}")
        if player.get('phone'):
            st.write(f"**Phone:** {player['phone']}")
        if player.get('parent_email'):
            st.write(f"**Parent Email:** {player['parent_email']}")
    
    # Tabs for different views
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📅 Sessions", "⚾ Pitches", "👨‍🏫 Coaches", "📍 Locations", "📊 Analytics"])
    
    with tab1:
        st.header("Training Sessions")
        sessions = get_player_sessions(conn, player_id)
        
        if sessions:
            # Display sessions in a more interactive format
            for session in sessions:
                with st.container():
                    col1, col2, col3, col4, col5 = st.columns([1, 2, 2, 3, 1])
                    
                    with col1:
                        st.write(f"**{session['session_date'].strftime('%m/%d/%Y')}**")
                    
                    with col2:
                        st.write(f"{session['session_type']}")
                        st.caption(f"{session['location'] or 'N/A'}")
                    
                    with col3:
                        st.write(f"Coach: {session['coach_name'] or 'N/A'}")
                        st.caption(f"Source: {session['source_name']}")
                    
                    with col4:
                        metrics_text = []
                        if session['pitch_count']:
                            metrics_text.append(f"**{session['pitch_count']}** pitches")
                        if session['avg_velocity']:
                            metrics_text.append(f"Avg: **{session['avg_velocity']:.1f}** mph")
                        if session['max_velocity']:
                            metrics_text.append(f"Max: **{session['max_velocity']:.1f}** mph")
                        if session['avg_spin']:
                            metrics_text.append(f"Spin: **{session['avg_spin']:.0f}** rpm")
                        st.write(" | ".join(metrics_text) if metrics_text else "No pitch data")
                        
                        # Add pitch type breakdown if available
                        if session['pitch_count'] and session['pitch_count'] > 0:
                            pitch_types = get_pitch_type_summary(conn, session['session_id'])
                            if pitch_types:
                                type_summary = ", ".join([f"{pt['pitch_type']}: {pt['count']}" for pt in pitch_types])
                                st.caption(f"📊 {type_summary}")
                    
                    with col5:
                        if st.button("View", key=f"session_{session['session_id']}", width='stretch'):
                            # Store the selected session ID in session state
                            st.session_state['selected_session_id'] = session['session_id']
                            # Navigate to Session Detail page
                            st.switch_page("pages/2_Session_Detail.py")
                    
                    st.divider()
            
            st.info("💡 Tip: Click 'View' to see detailed session information")
        else:
            st.info("No sessions found for this player")
    
    with tab2:
        st.header("Recent Pitches")
        
        col1, col2 = st.columns([3, 1])
        with col2:
            pitch_limit = st.selectbox("Number of pitches to display", [50, 100, 250, 500, 1000], index=1)
        
        pitches = get_player_pitch_data(conn, player_id, pitch_limit)
        
        if pitches:
            pitch_data = []
            for pitch in pitches:
                pitch_data.append({
                    'Session ID': pitch['session_id'],
                    'Date': pitch['session_date'].strftime('%m/%d/%Y'),
                    'Pitch #': pitch['pitch_number'],
                    'Type': pitch['session_type'],
                    'Velocity': f"{pitch['release_speed']:.1f}" if pitch['release_speed'] else 'N/A',
                    'Spin Rate': f"{pitch['spin_rate']:.0f}" if pitch['spin_rate'] else 'N/A',
                    'Spin Axis': f"{pitch['spin_axis']:.0f}°" if pitch['spin_axis'] else 'N/A',
                    'H Break': f"{pitch['horizontal_break']:.1f}" if pitch['horizontal_break'] else 'N/A',
                    'V Break': f"{pitch['induced_vertical_break']:.1f}" if pitch['induced_vertical_break'] else 'N/A',
                    'Release Height': f"{pitch['release_height']:.2f}" if pitch['release_height'] else 'N/A',
                    'Extension': f"{pitch['release_extension']:.2f}" if pitch['release_extension'] else 'N/A',
                })
            
            df = pd.DataFrame(pitch_data)
            st.dataframe(df, width='stretch', hide_index=True)
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download Pitch Data as CSV",
                data=csv,
                file_name=f"{player['first_name']}_{player['last_name']}_pitches.csv",
                mime="text/csv"
            )
        else:
            st.info("No pitch data found for this player")
    
    with tab3:
        st.header("Coaches")
        coaches = get_player_coaches(conn, player_id)
        
        if coaches:
            for coach in coaches:
                with st.container():
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.subheader(coach['coach_name'])
                        if coach.get('organization'):
                            st.write(f"**Organization:** {coach['organization']}")
                        if coach.get('email'):
                            st.write(f"**Email:** [{coach['email']}](mailto:{coach['email']})")
                        if coach.get('phone'):
                            st.write(f"**Phone:** [{coach['phone']}](tel:{coach['phone']})")
                    
                    with col2:
                        st.metric("Sessions Together", coach['session_count'])
                        if coach['first_session']:
                            st.write(f"**First:** {coach['first_session'].strftime('%m/%d/%Y')}")
                        if coach['last_session']:
                            st.write(f"**Last:** {coach['last_session'].strftime('%m/%d/%Y')}")
                    
                    if st.button(f"View Coach Profile", key=f"coach_{coach['coach_id']}"):
                        st.info("💡 Go to Coach Profile page and select this coach")
                    
                    st.markdown("---")
        else:
            st.info("No coaches associated with this player yet")
    
    with tab4:
        st.header("Training Locations")
        locations = get_player_locations(conn, player_id)
        
        if locations:
            location_data = []
            for loc in locations:
                location_data.append({
                    'Location': loc['location'],
                    'Sessions': loc['session_count'],
                    'Total Pitches': loc['pitch_count'],
                    'First Visit': loc['first_visit'].strftime('%m/%d/%Y'),
                    'Last Visit': loc['last_visit'].strftime('%m/%d/%Y')
                })
            
            df = pd.DataFrame(location_data)
            st.dataframe(df, width='stretch', hide_index=True)
            
            # Simple bar chart of sessions by location
            fig = px.bar(df, x='Location', y='Sessions', 
                        title='Sessions by Location',
                        labels={'Sessions': 'Number of Sessions'})
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No location data recorded for this player")
    
    with tab5:
        st.header("Analytics")
        
        # Get pitch data for analytics
        pitches = get_player_pitch_data(conn, player_id, 1000)
        
        if pitches and len(pitches) > 0:
            # Create DataFrame
            df = pd.DataFrame(pitches)
            
            # Convert session_date to datetime for Plotly compatibility
            if 'session_date' in df.columns:
                df['session_date'] = pd.to_datetime(df['session_date'])
            
            if 'pitch_type' in df.columns:
                st.subheader("Pitch Type Breakdown")
                pitch_type_counts = df['pitch_type'].value_counts()
    
                # Display pitch types as clickable buttons
                cols = st.columns(min(len(pitch_type_counts), 5))
                for idx, (pitch_type, count) in enumerate(pitch_type_counts.items()):
                    with cols[idx % 5]:
                        if st.button(f"{pitch_type}\n{count} pitches", key=f"pt_{pitch_type}"):
                            st.session_state['selected_player_id'] = player_id
                            st.session_state['selected_pitch_type'] = pitch_type
                            st.switch_page("pages/4_Pitch_Type_Analysis.py")
            
            # Check if we have pitch type data
            has_pitch_types = 'pitch_type' in df.columns and df['pitch_type'].notna().any()
            
            if has_pitch_types:
                # Get unique pitch types
                pitch_types = df[df['pitch_type'].notna()]['pitch_type'].unique()
                
                # Add pitch type filter
                st.subheader("🎯 Filter by Pitch Type")
                selected_types = st.multiselect(
                    "Select pitch types to display (leave empty for all)",
                    options=list(pitch_types),
                    default=list(pitch_types),
                    key="analytics_pitch_type_filter"
                )
            
                # Filter data if selections made
                if selected_types:
                    df_filtered = df[df['pitch_type'].isin(selected_types)]
                else:
                    df_filtered = df
                
                st.markdown("---")
            else:
                df_filtered = df
                selected_types = None
            
            # Velocity over time
            if 'release_speed' in df_filtered.columns and df_filtered['release_speed'].notna().any():
                st.subheader("📈 Velocity Trends")
                
                if has_pitch_types and selected_types:
                    # Color by pitch type
                    fig = px.scatter(df_filtered, x='session_date', y='release_speed',
                                   color='pitch_type',
                                   title='Velocity Over Time by Pitch Type',
                                   labels={'session_date': 'Date', 'release_speed': 'Velocity (mph)', 'pitch_type': 'Pitch Type'},
                                   trendline='lowess',
                                   trendline_scope='overall')
                else:
                    fig = px.scatter(df_filtered, x='session_date', y='release_speed',
                                   title='Velocity Over Time',
                                   labels={'session_date': 'Date', 'release_speed': 'Velocity (mph)'},
                                   trendline='lowess')
                
                st.plotly_chart(fig, width='stretch')
                
                # Velocity stats by pitch type
                if has_pitch_types and selected_types:
                    st.subheader("Velocity by Pitch Type")
                    
                    cols = st.columns(min(len(selected_types), 4))
                    for idx, pitch_type in enumerate(selected_types):
                        pt_data = df_filtered[df_filtered['pitch_type'] == pitch_type]['release_speed']
                        if len(pt_data) > 0:
                            with cols[idx % len(cols)]:
                                st.metric(
                                    f"{pitch_type}",
                                    f"{pt_data.mean():.1f} mph",
                                    f"Max: {pt_data.max():.1f}"
                                )
                else:
                    # Overall velocity stats
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Average Velocity", f"{df_filtered['release_speed'].mean():.1f} mph")
                        st.metric("Max Velocity", f"{df_filtered['release_speed'].max():.1f} mph")
                    with col2:
                        st.metric("Min Velocity", f"{df_filtered['release_speed'].min():.1f} mph")
                        st.metric("Std Deviation", f"{df_filtered['release_speed'].std():.1f} mph")
            
            # Spin rate over time
            if 'spin_rate' in df_filtered.columns and df_filtered['spin_rate'].notna().any():
                st.subheader("🌀 Spin Rate Trends")
                
                if has_pitch_types and selected_types:
                    fig = px.scatter(df_filtered, x='session_date', y='spin_rate',
                                   color='pitch_type',
                                   title='Spin Rate Over Time by Pitch Type',
                                   labels={'session_date': 'Date', 'spin_rate': 'Spin Rate (rpm)', 'pitch_type': 'Pitch Type'},
                                   trendline='lowess',
                                   trendline_scope='overall')
                else:
                    fig = px.scatter(df_filtered, x='session_date', y='spin_rate',
                                   title='Spin Rate Over Time',
                                   labels={'session_date': 'Date', 'spin_rate': 'Spin Rate (rpm)'},
                                   trendline='lowess')
                
                st.plotly_chart(fig, width='stretch')
            
            # Movement plot
            if 'horizontal_break' in df_filtered.columns and 'induced_vertical_break' in df_filtered.columns:
                df_movement = df_filtered[df_filtered['horizontal_break'].notna() & df_filtered['induced_vertical_break'].notna()]
                if len(df_movement) > 0:
                    st.subheader("⚾ Pitch Movement")
                    
                    if has_pitch_types and selected_types:
                        fig = px.scatter(df_movement, x='horizontal_break', y='induced_vertical_break',
                                       color='pitch_type',
                                       title='Pitch Movement Profile by Pitch Type',
                                       labels={'horizontal_break': 'Horizontal Break (in)', 
                                              'induced_vertical_break': 'Induced Vertical Break (in)',
                                              'pitch_type': 'Pitch Type'},
                                       opacity=0.6)
                    else:
                        fig = px.scatter(df_movement, x='horizontal_break', y='induced_vertical_break',
                                       title='Pitch Movement Profile',
                                       labels={'horizontal_break': 'Horizontal Break (in)', 
                                              'induced_vertical_break': 'Induced Vertical Break (in)'},
                                       opacity=0.6) 
                    fig.add_hline(y=0, line_dash="dash", line_color="gray")
                    fig.add_vline(x=0, line_dash="dash", line_color="gray")
                    st.plotly_chart(fig, width='stretch')
        else:
            st.info("No pitch data available for analytics")
    
    conn.close()

if __name__ == "__main__":
    main()
