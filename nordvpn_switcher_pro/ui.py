import os
from typing import List, Dict, Union, Any
from .exceptions import ConfigurationError, ApiClientError

import questionary
from questionary import Choice


def clear_screen():
    """Clears the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')


def get_custom_style():
    """Returns a custom style for questionary prompts."""
    return questionary.Style([
        ('qmark', 'fg:#673ab7 bold'),       # Question mark
        ('question', 'bold'),               # Question text
        ('pointer', 'fg:#cc5454 bold'),      # Pointer to current choice
        ('highlighted', 'fg:#cc5454 bold'),  # Highlighted choice
        ('selected', 'fg:#ffffff bg:#673ab7 bold'), # Selected choice
        ('answer', 'fg:#f44336 bold'),      # Answer text
        ('separator', 'fg:#9e9e9e'),
        ('instruction', 'fg:#ffeb3b')
    ])


def prompt_main_menu() -> str:
    """Displays the main menu for connection type selection."""
    clear_screen()
    return questionary.select(
        "How would you like to connect?",
        choices=[
            Choice("To a specific Country (or a sequence of countries)", "country"),
            Choice("To a specific City (or a sequence of cities)", "city"),
            Choice("To a specific Region (e.g., Europe, The Americas, Custom Region)", "region"),
            Choice("To a random server worldwide (Standard servers only)", "worldwide"),
            Choice("To a Special Server Group (e.g., P2P, Double VPN)", "special"),
            Choice("Exit", "exit")
        ],
        style=get_custom_style(),
        instruction="(Use arrow keys to navigate, <enter> to select; Click into the terminal if needed)"
    ).ask()


def prompt_country_id_input(countries: List[Dict]) -> List[int]:
    """
    Displays a formatted list of countries and prompts the user to enter IDs.
    This is used for the 'country' main menu choice.
    """
    clear_screen()
    print("\x1b[1m\x1b[36m--- Select Country/Countries by ID or Name ---\x1b[0m\n")

    sorted_countries = sorted(countries, key=lambda x: x['name'])
    
    header = f"{'ID':<5} | {'Country':<25} | {'Servers':<10}"
    print(header)
    print("-" * len(header))
    for country in sorted_countries:
        print(f"\x1b[96m{country['id']:<5}\x1b[0m | {country['name']:<25} | {country['serverCount']:<10}")
    print("-" * len(header))
    print("\n\x1b[33mServers will be rotated through each country sequentially (e.g., all of Germany, then all of France).\x1b[0m\n")

    # Build lookup dictionaries for fast validation
    id_to_country = {str(c['id']): c for c in countries}
    name_to_id = {c['name'].lower(): c['id'] for c in countries}

    def validate_input(text):
        if not text:
            return "Please enter at least one country ID or name."
        parts = [part.strip() for part in text.split(",") if part.strip()]
        unrecognized = []
        for part in parts:
            if part.isdigit():
                if part not in id_to_country:
                    unrecognized.append(part)
            else:
                if part.lower() not in name_to_id:
                    unrecognized.append(part)
        if unrecognized:
            return f"Unrecognized: {', '.join(unrecognized)}. Please check spelling or use valid IDs/names."
        return True

    ids_input = questionary.text(
        "Enter one or more country IDs or names from the list above, separated by commas:",
        validate=validate_input,
        style=get_custom_style()
    ).ask()

    if not ids_input:
        return []

    result_ids = []
    for part in [p.strip() for p in ids_input.split(",") if p.strip()]:
        if part.isdigit():
            result_ids.append(int(part))
        else:
            result_ids.append(name_to_id[part.lower()])
    return result_ids


def prompt_city_id_input(countries: List[Dict]) -> List[int]:
    """
    Displays a formatted list of country -> city entries and prompts the user to enter city IDs.
    Accepts either numeric city IDs or city names. Country names may be used to help display only.
    """
    clear_screen()
    print("\x1b[1m\x1b[36m--- Select City/Cities by ID or Name ---\x1b[0m\n")

    # Build a flat list of (country, city) dicts for display and lookup
    rows = []
    for country in sorted(countries, key=lambda x: x['name']):
        for city in sorted(country.get('cities', []), key=lambda c: c['name']):
            rows.append({
                'country_name': country['name'],
                'country_id': country.get('id'),
                'city_id': city['id'],
                'city_name': city['name'],
                'serverCount': city.get('serverCount', 0)
            })

    header = f"{'ID':<7} | {'Country':<25} | {'City':<30} | {'Servers':<8}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"\x1b[96m{r['city_id']:<7}\x1b[0m | "
            f"{r['country_name']:<25} | "
            f"{r['city_name']:<30} | "
            f"{r['serverCount']:<8}"
        )
    print("-" * len(header))
    print("\n\x1b[33mServers will be rotated through each city sequentially (e.g., all servers in Berlin, then New York).\x1b[0m\n")

    id_to_row = {str(r['city_id']): r for r in rows}
    name_to_id = {r['city_name'].lower(): r['city_id'] for r in rows}

    def validate_input(text):
        if not text:
            return "Please enter at least one city ID or name."
        parts = [part.strip() for part in text.split(",") if part.strip()]
        unrecognized = []
        for part in parts:
            if part.isdigit():
                if part not in id_to_row:
                    unrecognized.append(part)
            else:
                if part.lower() not in name_to_id:
                    unrecognized.append(part)
        if unrecognized:
            return f"Unrecognized: {', '.join(unrecognized)}. Please check spelling or use valid IDs/names."
        return True

    ids_input = questionary.text(
        "Enter one or more city IDs or names from the list above, separated by commas:",
        validate=validate_input,
        style=get_custom_style()
    ).ask()

    if not ids_input:
        return []

    result_ids = []
    for part in [p.strip() for p in ids_input.split(",") if p.strip()]:
        if part.isdigit():
            result_ids.append(int(part))
        else:
            result_ids.append(name_to_id[part.lower()])
    return result_ids


def prompt_country_selection_multi(countries: List[Dict]) -> List[int]:
    """
    Displays an interactive, scrollable, multi-select list of countries.
    This is used for creating custom regions.
    """
    clear_screen()
    sorted_countries = sorted(countries, key=lambda x: x['name'])
    
    choices = [
        Choice(
            title=f"{c['name']:<25} ({c['serverCount']} servers)",
            value=c['id']
        ) for c in sorted_countries
    ]
    
    def validate_selection(selected):
        if not selected or len(selected) == 0:
            return "Please select at least one country."
        return True
    
    selected_ids = questionary.checkbox(
        "Select the countries for your custom region:",
        choices=choices,
        style=get_custom_style(),
        validate=validate_selection,
        instruction="(Use arrow keys to navigate, <space> to select, <a> to toggle, <i> to invert, <enter> to confirm)"
    ).ask()
    
    return selected_ids if selected_ids else []


def prompt_city_selection_multi(countries: List[Dict]) -> List[int]:
    """
    Interactive multi-select for cities. Displays entries grouped/sorted by country
    with the country and city shown in separate columns. Returns a list of city IDs (ints).
    """
    clear_screen()

    # Build flat list sorted by country then city
    flat = []
    for country in sorted(countries, key=lambda x: x['name']):
        for city in sorted(country.get('cities', []), key=lambda c: c['name']):
            flat.append((country['name'], city))

    # Column header
    header = f"{'Country':<25} | {'City':<30} | {'Srv':>4}"
    separator = "-" * len(header)

    choices = [
        questionary.Separator(header),
        questionary.Separator(separator)
    ]

    # Add choices
    for country_name, city in flat:
        title = f"{country_name:<25} | {city['name']:<30} | {city.get('serverCount', 0):>4}"
        choices.append(Choice(title=title, value=city['id']))

    def validate_selection(selected):
        if not selected or len(selected) == 0:
            return "Please select at least one city."
        return True

    selected_ids = questionary.checkbox(
        "Select the cities for your custom region:",
        choices=choices,
        style=get_custom_style(),
        validate=validate_selection,
        instruction="(Use arrow keys to navigate, <space> to select, <a> to toggle, <i> to invert, <enter> to confirm)"
    ).ask()

    return selected_ids if selected_ids else []


def prompt_group_selection(groups: List[Dict], group_type: str) -> Union[int, str, None]:
    """Displays a selection list for server groups (Regions or Special)."""
    clear_screen()

    # We only modify the logic for 'regions' to add custom options
    if group_type == 'regions':
        message = "Select a region:"
        # Fetch existing regions
        region_groups = [g for g in groups if g.get('type', {}).get('identifier') == 'regions']
        
        choices = [Choice(title=g['title'], value=g['id']) for g in region_groups]

        choices.append(questionary.Separator("--- Or Create a Custom Region ---"))
        choices.append(Choice(title="Include a specific list of countries", value="custom_region_in"))
        choices.append(Choice(title="Exclude a specific list of countries", value="custom_region_ex"))
        choices.append(Choice(title="Include a specific list of cities", value="custom_region_city"))
        choices.append(Choice(title="Cancel", value="exit"))

    else: # For 'special' groups, the logic is simpler
        message = "Select a special server group:"
        choices = [
            Choice(
                title=f"{g['title']} ({g.get('serverCount', 'N/A')} servers)", 
                value=g['id']
            ) for g in groups # 'groups' is now the pre-filtered list
        ]
        choices.append(Choice("Cancel", "exit"))

    return questionary.select(
        message,
        choices=choices,
        style=get_custom_style(),
        instruction="(Use arrow keys to navigate, <enter> to select)"
    ).ask()


def prompt_connection_strategy(possible_strats: List[str] = None) -> str:
    """Asks the user for their preferred server selection strategy.

    Args:
        possible_strats: Optional list of allowed strategy identifiers. If None,
            defaults to the original "standard" strategies ['recommended', 'randomized_load'].

    Returns:
        The chosen strategy identifier, or 'exit' if cancelled.
    """
    clear_screen()

    # Strategy metadata: id -> (title, description)
    STRAT_INFO = {
        'recommended': (
            "Best available (recommended for IP rotation)",
            "Uses NordVPN's algorithm based on distance from you and server load."
        ),
        'randomized_load': (
            "Randomized by load (recommended for simple Geo rotation)",
            "Picks randomly from all of your selected servers, prioritizing lower load."
        ),
        'randomized_country': (
            "Randomized by country (Geo rotation with a different country each rotation)",
            "Ensures each rotation connects to a different country than the previous rotation."
        ),
        'randomized_city': (
            "Randomized by city (Geo rotation with a different city each rotation)",
            "Ensures each rotation connects to a different city than the previous rotation."
        ),
    }

    # Default to the two original strategies if nothing specified
    if possible_strats is None:
        possible_strats = ['recommended', 'randomized_load']

    # Build choices preserving an order: recommended, randomized_load, randomized_country, randomized_city
    order = ['recommended', 'randomized_load', 'randomized_country', 'randomized_city']
    choices = []
    for sid in order:
        if sid in possible_strats and sid in STRAT_INFO:
            title, desc = STRAT_INFO[sid]
            choices.append(Choice(title=title, value=sid, description=desc))

    choices.append(Choice("Cancel", "exit"))

    return questionary.select(
        "How should servers be selected?",
        choices=choices,
        style=get_custom_style(),
        instruction="(Use arrow keys to navigate, <enter> to select)"
    ).ask()


def prompt_special_server_retry() -> int:
    """Asks if the tool should re-rotate if a used special server is connected."""
    clear_screen()
    print("\x1b[1m\x1b[36mDue to CLI limitations, we can't choose the special server we get connected to. But we can retry if we detect a used IP.\x1b[0m\n")

    answer = questionary.text(
        "If a used server is connected, should we retry? (Y=5 retries)",
        default="Y",
        style=get_custom_style(),
        instruction="(Y/N, or a number for max retries)"
    ).ask()

    if answer.lower() in ['y', 'yes']:
        return 5
    if answer.lower() in ['n', 'no']:
        return 0
    try:
        return int(answer)
    except (ValueError, TypeError):
        print("Invalid input, defaulting to 5 retries.")
        return 5


def prompt_auto_disconnect_on_start() -> bool:
    """Asks whether start_session should disconnect any existing VPN connection."""
    clear_screen()
    return questionary.confirm(
        "Disconnect current VPN connection when starting a new session?",
        default=True,
        style=get_custom_style(),
        instruction="(Recommended for a clean start; disable if you rely on Kill Switch continuity)"
    ).ask()


def get_user_criteria(api_client) -> tuple[Dict[str, Any], List[Dict]]:
    """
    Guides the user through the entire interactive setup process.

    This function orchestrates all other prompt functions to build a complete
    criteria dictionary for the VpnSwitcher.

    Args:
        api_client: An instance of NordVpnApiClient to fetch data.

    Returns:
        A tuple containing:
        - A dictionary with the user's final connection criteria.
        - A list of countries (may be empty for flows that don't need it).
          Used for preflight checks that need names/ids lookup.
    """
    def _handle_cancel(result):
        """Centralized cancellation handler."""
        if result is None or result == 'exit':
            raise SystemExit("Setup cancelled by user.")
    
    countries = []
    main_choice = prompt_main_menu()
    _handle_cancel(main_choice)

    criteria = {"main_choice": main_choice}

    # Main choice handling
    match main_choice:
        # --- Country Selection ---
        case 'country':
            countries = api_client.get_countries()
            selected_ids = prompt_country_id_input(countries)
            if not selected_ids:
                raise ConfigurationError("No countries were selected. Aborting setup.")
            criteria['country_ids'] = selected_ids
            # Determine if randomized_city is possible: every selected country must have
            # more than one city available. If any selected country doesn't, only
            # offer the standard strategies.
            id_map = {c['id']: c for c in countries}
            can_randomize_city = True
            for cid in selected_ids:
                country_obj = id_map.get(cid, {})
                cities = country_obj.get('cities') or []
                if len(cities) < 2:
                    can_randomize_city = False
                    break

            possible = ['recommended', 'randomized_load']
            if can_randomize_city:
                possible.append('randomized_city')

            strategy = prompt_connection_strategy(possible)
            _handle_cancel(strategy)
            criteria['strategy'] = strategy

        # --- City Selection ---
        case 'city':
            countries = api_client.get_countries()
            selected_ids = prompt_city_id_input(countries)
            if not selected_ids:
                raise ConfigurationError("No cities were selected. Aborting setup.")
            criteria['city_ids'] = selected_ids
            # For city-based selection only the standard strategies are available
            possible = ['recommended', 'randomized_load']
            strategy = prompt_connection_strategy(possible)
            _handle_cancel(strategy)
            criteria['strategy'] = strategy

        case 'region':
            # --- Region / Custom Region Selection ---
            groups = api_client.get_groups()
            region_choice = prompt_group_selection(groups, 'regions')
            _handle_cancel(region_choice)

            if isinstance(region_choice, str) and region_choice.startswith('custom_region'):
                criteria['main_choice'] = region_choice
                countries = api_client.get_countries()
                # If the user chose a custom city region, prompt for cities
                if region_choice == 'custom_region_city':
                    selected_ids = prompt_city_selection_multi(countries)
                    if not selected_ids:
                        raise ConfigurationError("No cities were selected for the custom region. Aborting setup.")
                    criteria['city_ids'] = selected_ids
                else:
                    selected_ids = prompt_country_selection_multi(countries)
                    if not selected_ids:
                        raise ConfigurationError("No countries were selected for the custom region. Aborting setup.")
                    criteria['country_ids'] = selected_ids

                # Build pools of available countries and cities based on the custom selection.
                # For 'custom_region_in' the pool is exactly the selected countries.
                # For 'custom_region_ex' the pool is all countries except the selected ones.
                # For 'custom_region_city' the pool is the selected cities and their parent countries.
                pool_country_ids = set()
                pool_city_ids = set()

                if region_choice == 'custom_region_city':
                    # selected_ids are city ids
                    pool_city_ids = set(selected_ids)
                    # Determine parent countries for the selected cities
                    for country in countries:
                        for city in country.get('cities', []):
                            if city['id'] in pool_city_ids:
                                pool_country_ids.add(country['id'])
                else:
                    # selected_ids are country ids
                    all_country_ids = {c['id'] for c in countries}
                    if region_choice == 'custom_region_ex':
                        pool_country_ids = all_country_ids - set(selected_ids)
                    else:  # custom_region_in
                        pool_country_ids = set(selected_ids)

                    # collect all cities that belong to the pool countries
                    for country in countries:
                        if country['id'] in pool_country_ids:
                            for city in country.get('cities', []):
                                pool_city_ids.add(city['id'])

                # Decide which randomized strategies are possible
                possible = ['recommended', 'randomized_load']
                if len(pool_country_ids) > 1:
                    possible.append('randomized_country')
                if len(pool_city_ids) > 1:
                    possible.append('randomized_city')

                strategy = prompt_connection_strategy(possible)
                _handle_cancel(strategy)
                criteria['strategy'] = strategy
            else:
                criteria['group_id'] = region_choice

                # Note: when a pre-defined region group (group_id) is selected above
                # we fall through to here and will handle offering all strategies below.

            # If the user picked a standard predefined region (not custom), allow all strategies
            if not isinstance(region_choice, str) or not region_choice.startswith('custom_region'):
                possible = ['recommended', 'randomized_load', 'randomized_country', 'randomized_city']
                strategy = prompt_connection_strategy(possible)
                _handle_cancel(strategy)
                criteria['strategy'] = strategy

        # --- Worldwide Selection ---
        case 'worldwide':
            # All strategies possible for worldwide
            possible = ['recommended', 'randomized_load', 'randomized_country', 'randomized_city']
            strategy = prompt_connection_strategy(possible)
            _handle_cancel(strategy)
            criteria['strategy'] = strategy

        # -- Special Server Group Selection ---
        case 'special':
            all_groups = api_client.get_groups()

            print("\n\x1b[33mChecking availability of special server groups...\x1b[0m")
            special_groups = []
            for group in all_groups:
                # Filter for "Legacy category" and exclude "Standard VPN servers"
                if group.get('type', {}).get('identifier') == 'legacy_group_category' and \
                group.get('identifier') != 'legacy_standard':
                    try:
                        count_data = api_client.get_group_server_count(group['id'])
                        if count_data.get('count', 0) > 0:
                            # Add the server count to the group object for display
                            group['serverCount'] = count_data.get('count')
                            special_groups.append(group)
                    except ApiClientError:
                        # If the count endpoint fails for a group, just skip it
                        continue

            if not special_groups:
                raise ConfigurationError("Could not find any available special server groups at the moment.")

            # Now prompt the user with the filtered, available list
            group_id = prompt_group_selection(special_groups, 'special')
            _handle_cancel(group_id)

            group_details = next((g for g in all_groups if g['id'] == group_id), {})

            criteria['group_identifier'] = group_details.get('identifier')
            criteria['group_title'] = group_details.get('title')
            retry_count = prompt_special_server_retry()
            _handle_cancel(retry_count)
            criteria['retry_count'] = retry_count

    # Final validation to ensure a strategy was selected where needed
    if main_choice != 'special' and not criteria.get('strategy'):
        raise ConfigurationError("Connection strategy was not selected. Aborting setup.")

    auto_disconnect = prompt_auto_disconnect_on_start()
    _handle_cancel(auto_disconnect)
    criteria['auto_disconnect_on_start'] = bool(auto_disconnect)

    return criteria, countries


def display_critical_error(reason: str):
    """Displays a standardized critical error message for application failures."""
    print("\n" + "="*60)
    print("\x1b[91mCRITICAL ERROR: A VPN connection could not be established.\x1b[0m")
    print(f"\x1b[91mReason: {reason}\x1b[0m")
    print("\x1b[33mThis often happens if the NordVPN application is stuck or unresponsive.\x1b[0m")
    print("\x1b[33m\nRecommended Action:\x1b[0m")
    print("  1. Close NordVPN completely from your system tray (Right-click -> Quit).")
    print("  2. Restart your script.")
    print("\n\x1b[33mTo force-close from the command line, you can use:\x1b[0m")
    print("  taskkill /F /IM nordvpn.exe /T")
    print("="*60)