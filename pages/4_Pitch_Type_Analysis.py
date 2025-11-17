import streamlit as st
import mysql.connector
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# Page config
st.set_page_config(page_title="Pitch Type Analysis", layout="wide")

def get_db_connection():
    """Create database connection"""
    return mysql.connector.connect(
        host=st.secrets["DB_HOST"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        database=st.secrets["DB_NAME"]
    )

def get_all_players():
    """Get list of all players"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT player_id, first_name, last_name
    FROM players
    ORDER BY last_name, first_name
    """
    
    cursor.execute(query)
    players = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return players

def get_player_pitch_types(player_id):
    """Get all pitch types thrown by a player"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT DISTINCT pd.pitch_type, COUNT(*) as count
    FROM pitch_data pd
    JOIN training_sessions ts ON pd.session_id = ts.session_id
    WHERE ts.player_id = %s AND pd.pitch_type IS NOT NULL
    GROUP BY pd.pitch_type
    ORDER BY count DESC
    """
    
    cursor.execute(query, (player_id,))
    pitch_types = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return pitch_types

def get_player_sessions(player_id):
    """Get all sessions for a player"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = """
    SELECT 
        ts.session_id,
        ts.session_date,
        ts.location,
        COUNT(pd.pitch_id) as pitch_count
    FROM training_sessions ts
    LEFT JOIN pitch_data pd ON ts.session_id = pd.session_id
    WHERE ts.player_id = %s
    GROUP BY ts.session_id, ts.session_date, ts.location
    ORDER BY ts.session_date DESC
    """
    
    cursor.execute(query, (player_id,))
    sessions = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return sessions

def get_pitch_type_data(player_id, pitch_type, session_ids=None):
    """Get all pitches of a specific type for a player, optionally filtered by sessions"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    if session_ids:
        placeholders = ','.join(['%s'] * len(session_ids))
        query = f"""
        SELECT pd.*, ts.session_date, ts.session_id, pl.throws_hand
        FROM pitch_data pd
        JOIN training_sessions ts ON pd.session_id = ts.session_id
        JOIN players pl ON ts.player_id = pl.player_id
        WHERE ts.player_id = %s AND pd.pitch_type = %s 
        AND ts.session_id IN ({placeholders})
        ORDER BY pd.pitch_timestamp
        """
        cursor.execute(query, (player_id, pitch_type) + tuple(session_ids))
    else:
        query = """
        SELECT pd.*, ts.session_date, ts.session_id, pl.throws_hand
        FROM pitch_data pd
        JOIN training_sessions ts ON pd.session_id = ts.session_id
        JOIN players pl ON ts.player_id = pl.player_id
        WHERE ts.player_id = %s AND pd.pitch_type = %s
        ORDER BY pd.pitch_timestamp
        """
        cursor.execute(query, (player_id, pitch_type))
    
    pitches = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return pitches

def create_combined_polar_chart(pitches_df, throws_hand):
    """Create combined arm slot and spin direction polar chart"""
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
    
    # Plot arm slots (blue)
    arm_slots = pitches_df[pitches_df['arm_slot'].notna()]['arm_slot'].astype(float).tolist()
    if arm_slots:
        fig.add_trace(go.Scatterpolar(
            r=[0.9] * len(arm_slots),
            theta=arm_slots,
            mode='markers',
            marker=dict(size=8, color='blue', opacity=0.6),
            name='Arm Slot',
            showlegend=True
        ))
    
    # Plot spin directions (red)
    spin_axes = pitches_df[pitches_df['spin_axis'].notna()]['spin_axis'].astype(float).tolist()
    if spin_axes:
        fig.add_trace(go.Scatterpolar(
            r=[1.0] * len(spin_axes),
            theta=spin_axes,
            mode='markers',
            marker=dict(size=8, color='red', opacity=0.6),
            name='Spin Direction',
            showlegend=True
        ))
    
    # Determine arm side and glove side labels based on throwing hand
    if throws_hand == 'R':
        arm_side = "9:00 (Arm Side)"
        glove_side = "3:00 (Glove Side)"
    else:
        arm_side = "3:00 (Arm Side)"
        glove_side = "9:00 (Glove Side)"
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=False, range=[0, 1.1]),
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

def create_spin_rate_over_time(pitches_df):
    """Create spin rate progression over sessions"""
    if len(pitches_df) == 0 or 'spin_rate' not in pitches_df.columns:
        return None
    
    pitches_df = pitches_df.dropna(subset=['spin_rate'])
    
    if len(pitches_df) == 0:
        return None
    
    # Group by session
    session_stats = pitches_df.groupby('session_date').agg({
        'spin_rate': ['mean', 'min', 'max']
    }).reset_index()
    
    session_stats.columns = ['session_date', 'avg_spin', 'min_spin', 'max_spin']
    session_stats = session_stats.sort_values('session_date')
    
    fig = go.Figure()
    
    # Add average line
    fig.add_trace(go.Scatter(
        x=session_stats['session_date'],
        y=session_stats['avg_spin'],
        mode='lines+markers',
        name='Average',
        line=dict(color='blue', width=3)
    ))
    
    # Add range band
    fig.add_trace(go.Scatter(
        x=session_stats['session_date'],
        y=session_stats['max_spin'],
        mode='lines',
        name='Max',
        line=dict(color='lightblue', width=1),
        showlegend=False
    ))
    
    fig.add_trace(go.Scatter(
        x=session_stats['session_date'],
        y=session_stats['min_spin'],
        mode='lines',
        name='Min',
        line=dict(color='lightblue', width=1),
        fill='tonexty',
        fillcolor='rgba(173, 216, 230, 0.3)',
        showlegend=False
    ))
    
    fig.update_layout(
        title='Spin Rate Progression Over Sessions',
        xaxis_title='Session Date',
        yaxis_title='Spin Rate (rpm)',
        height=400
    )
    
    return fig

def create_velocity_over_time(pitches_df):
    """Create velocity progression over sessions"""
    if len(pitches_df) == 0 or 'release_speed' not in pitches_df.columns:
        return None
    
    pitches_df = pitches_df.dropna(subset=['release_speed'])
    
    if len(pitches_df) == 0:
        return None
    
    # Group by session
    session_stats = pitches_df.groupby('session_date').agg({
        'release_speed': ['mean', 'min', 'max']
    }).reset_index()
    
    session_stats.columns = ['session_date', 'avg_velo', 'min_velo', 'max_velo']
    session_stats = session_stats.sort_values('session_date')
    
    fig = go.Figure()
    
    # Add average line
    fig.add_trace(go.Scatter(
        x=session_stats['session_date'],
        y=session_stats['avg_velo'],
        mode='lines+markers',
        name='Average',
        line=dict(color='green', width=3)
    ))
    
    # Add range band
    fig.add_trace(go.Scatter(
        x=session_stats['session_date'],
        y=session_stats['max_velo'],
        mode='lines',
        name='Max',
        line=dict(color='lightgreen', width=1),
        showlegend=False
    ))
    
    fig.add_trace(go.Scatter(
        x=session_stats['session_date'],
        y=session_stats['min_velo'],
        mode='lines',
        name='Min',
        line=dict(color='lightgreen', width=1),
        fill='tonexty',
        fillcolor='rgba(144, 238, 144, 0.3)',
        showlegend=False
    ))
    
    fig.update_layout(
        title='Velocity Progression Over Sessions',
        xaxis_title='Session Date',
        yaxis_title='Velocity (mph)',
        height=400
    )
    
    return fig

def create_movement_chart(pitches_df):
    """Create pitch movement scatter plot colored by session"""
    if len(pitches_df) == 0:
        return None
    
    # Filter to only pitches with both movement values
    movement_df = pitches_df.dropna(subset=['horz_break', 'induced_vert_break'])
    
    if len(movement_df) == 0:
        return None
    
    # Convert session_date to string for color grouping
    movement_df['session_label'] = movement_df['session_date'].astype(str)
    
    fig = px.scatter(movement_df, x='horz_break', y='induced_vert_break',
                     color='session_label',
                     title='Pitch Movement by Session',
                     labels={'horz_break': 'Horizontal Break (in)', 
                            'induced_vert_break': 'Induced Vertical Break (in)',
                            'session_label': 'Session'})
    
    # Add reference lines at 0
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    fig.add_vline(x=0, line_dash="dash", line_color="gray")
    
    fig.update_layout(height=400)
    return fig

# Main app
st.title("⚾ Pitch Type Analysis")

# Player and pitch type selection
st.markdown("### Select Player and Pitch Type")

col1, col2 = st.columns(2)

with col1:
    # Get all players
    players = get_all_players()
    player_options = {f"{p['last_name']}, {p['first_name']}": p['player_id'] for p in players}
    
    # Check if we have a pre-selected player
    selected_player_name = None
    if 'selected_player_id' in st.session_state:
        selected_player_name = next(
            (name for name, pid in player_options.items() if pid == st.session_state['selected_player_id']),
            None
        )
    
    selected_player_name = st.selectbox(
        "Player",
        options=list(player_options.keys()),
        index=list(player_options.keys()).index(selected_player_name) if selected_player_name else 0,
        key='player_selector'
    )
    
    selected_player_id = player_options[selected_player_name]

with col2:
    # Get pitch types for selected player
    pitch_types_data = get_player_pitch_types(selected_player_id)
    
    if not pitch_types_data:
        st.warning(f"No pitch data found for {selected_player_name}")
        st.stop()
    
    # Check if we have a pre-selected pitch type
    selected_pitch_type = st.session_state.get('selected_pitch_type', pitch_types_data[0]['pitch_type'])
    
    pitch_type_options = {f"{pt['pitch_type']} ({pt['count']} pitches)": pt['pitch_type'] 
                          for pt in pitch_types_data}
    
    selected_pitch_type_label = st.selectbox(
        "Pitch Type",
        options=list(pitch_type_options.keys()),
        index=list(pitch_type_options.values()).index(selected_pitch_type) if selected_pitch_type in pitch_type_options.values() else 0,
        key='pitch_type_selector'
    )
    
    selected_pitch_type = pitch_type_options[selected_pitch_type_label]

# Clear pre-selection from session state
if 'selected_player_id' in st.session_state:
    del st.session_state['selected_player_id']
if 'selected_pitch_type' in st.session_state:
    del st.session_state['selected_pitch_type']

# Session selection filter
st.markdown("### Filter by Sessions")
sessions = get_player_sessions(selected_player_id)

if sessions:
    session_options = {
        f"{s['session_date']} - {s.get('location', 'N/A')} ({s['pitch_count']} pitches)": s['session_id']
        for s in sessions
    }
    
    selected_session_labels = st.multiselect(
        "Select sessions to include (leave empty for all sessions)",
        options=list(session_options.keys()),
        default=[],
        key='session_filter'
    )
    
    selected_session_ids = [session_options[label] for label in selected_session_labels] if selected_session_labels else None
else:
    st.warning("No sessions found")
    selected_session_ids = None

# Get pitch type data
pitches = get_pitch_type_data(selected_player_id, selected_pitch_type, selected_session_ids)

if not pitches:
    st.warning(f"No {selected_pitch_type} pitches found for the selected sessions")
    st.stop()

# Convert to DataFrame
pitches_df = pd.DataFrame(pitches)
throws_hand = pitches_df['throws_hand'].iloc[0] if 'throws_hand' in pitches_df.columns else 'R'

# Display header
player_name = selected_player_name.split(', ')
st.markdown(f"## {player_name[1]} {player_name[0]}")
st.markdown(f"### {selected_pitch_type} Analysis")

# Summary statistics
st.subheader("Summary Statistics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Pitches", len(pitches_df))

with col2:
    if 'release_speed' in pitches_df.columns:
        avg_velo = pitches_df['release_speed'].mean()
        st.metric("Avg Velocity", f"{avg_velo:.1f} mph" if pd.notna(avg_velo) else "N/A")
    else:
        st.metric("Avg Velocity", "N/A")

with col3:
    if 'spin_rate' in pitches_df.columns:
        avg_spin = pitches_df['spin_rate'].mean()
        st.metric("Avg Spin Rate", f"{avg_spin:.0f} rpm" if pd.notna(avg_spin) else "N/A")
    else:
        st.metric("Avg Spin Rate", "N/A")

with col4:
    num_sessions = pitches_df['session_id'].nunique()
    st.metric("Sessions", num_sessions)

st.markdown("---")

# Detailed statistics by metric
st.subheader("Detailed Metrics")

# Velocity statistics
if 'release_speed' in pitches_df.columns:
    velo_data = pitches_df['release_speed'].dropna()
    if len(velo_data) > 0:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Min Velocity", f"{velo_data.min():.1f} mph")
        with col2:
            st.metric("Avg Velocity", f"{velo_data.mean():.1f} mph")
        with col3:
            st.metric("Max Velocity", f"{velo_data.max():.1f} mph")

# Spin rate statistics
if 'spin_rate' in pitches_df.columns:
    spin_data = pitches_df['spin_rate'].dropna()
    if len(spin_data) > 0:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Min Spin Rate", f"{spin_data.min():.0f} rpm")
        with col2:
            st.metric("Avg Spin Rate", f"{spin_data.mean():.0f} rpm")
        with col3:
            st.metric("Max Spin Rate", f"{spin_data.max():.0f} rpm")

# Movement statistics
if 'horz_break' in pitches_df.columns and 'induced_vert_break' in pitches_df.columns:
    st.markdown("#### Movement Statistics")
    col1, col2 = st.columns(2)
    
    with col1:
        hb_data = pitches_df['horz_break'].dropna()
        if len(hb_data) > 0:
            st.write("**Horizontal Break**")
            subcol1, subcol2, subcol3 = st.columns(3)
            with subcol1:
                st.metric("Min", f"{hb_data.min():.1f} in")
            with subcol2:
                st.metric("Avg", f"{hb_data.mean():.1f} in")
            with subcol3:
                st.metric("Max", f"{hb_data.max():.1f} in")
    
    with col2:
        ivb_data = pitches_df['induced_vert_break'].dropna()
        if len(ivb_data) > 0:
            st.write("**Induced Vertical Break**")
            subcol1, subcol2, subcol3 = st.columns(3)
            with subcol1:
                st.metric("Min", f"{ivb_data.min():.1f} in")
            with subcol2:
                st.metric("Avg", f"{ivb_data.mean():.1f} in")
            with subcol3:
                st.metric("Max", f"{ivb_data.max():.1f} in")

st.markdown("---")

# Charts
st.subheader("Analytics")

# Progression over time
col1, col2 = st.columns(2)

with col1:
    fig = create_velocity_over_time(pitches_df)
    if fig:
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Velocity data not available")

with col2:
    fig = create_spin_rate_over_time(pitches_df)
    if fig:
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Spin rate data not available")

# Release mechanics polar chart
st.subheader("Release Mechanics")
fig = create_combined_polar_chart(pitches_df, throws_hand)
if fig:
    st.plotly_chart(fig, width='stretch')
else:
    st.info("Release mechanics data not available")

# Movement chart
st.subheader("Pitch Movement by Session")
fig = create_movement_chart(pitches_df)
if fig:
    st.plotly_chart(fig, width='stretch')
else:
    st.info("Movement data not available")
