# Create searchable player list
player_options = {f"{p['player_name']} ({p['graduation_year']})": p['player_id'] 
                 for p in players}

# Type-ahead search box
search_term = st.text_input(
    "Search for a player (type name to filter)",
    placeholder="Start typing player name...",
    key="player_search"
)

# Filter players based on search
if search_term:
    filtered_players = [name for name in player_options.keys() 
                       if search_term.lower() in name.lower()]
else:
    filtered_players = list(player_options.keys())

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
