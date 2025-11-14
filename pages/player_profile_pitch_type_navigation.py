# Add this code snippet to pages_1_Player_Profile.py 
# to enable clicking on pitch types in the summary

# In the pitch type breakdown section, replace the static display with clickable buttons:

if 'pitch_type' in all_pitches_df.columns:
    st.subheader("Pitch Type Breakdown")
    pitch_type_counts = all_pitches_df['pitch_type'].value_counts()
    
    # Display pitch types as clickable buttons
    cols = st.columns(min(len(pitch_type_counts), 5))
    for idx, (pitch_type, count) in enumerate(pitch_type_counts.items()):
        with cols[idx % 5]:
            if st.button(f"{pitch_type}\n{count} pitches", key=f"pt_{pitch_type}"):
                st.session_state['selected_player_id'] = player_id
                st.session_state['selected_pitch_type'] = pitch_type
                st.switch_page("pages/4_Pitch_Type_Analysis.py")
