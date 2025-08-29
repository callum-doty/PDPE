import math


def calculate_distance(lat1, lon1, lat2, lon2):
    # Calculate distance between two points (simplified "Haversine" formula)
    # This returns distance in kilometers
    R = 6371  # Earth's radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance


def get_college_presence_score(cell_lat, cell_lon, time):
    """
    Calculates a college student presence score for a grid cell.
    """
    # 1. Define the important points (lat, lon, "weight")
    student_hotspots = [
        (39.0334, -94.5760, 1.5),  # UMKC Main Campus (high weight)
        (38.9584, -95.2448, 1.5),  # KU Main Campus
        (38.7440, -93.7310, 1.5),  # UCM Main Campus
        (39.0496, -94.5913, 1.2),  # Westport Bar District
        (39.0991, -94.5783, 1.1),  # Power & Light District (downtown KC)
        # Add more coordinates for libraries, popular cafes, etc.
    ]

    total_score = 0.0
    for hotspot_lat, hotspot_lon, weight in student_hotspots:
        dist_km = calculate_distance(cell_lat, cell_lon, hotspot_lat, hotspot_lon)

        # 2. Convert distance to a score. Closer = higher score.
        # A score that decays with distance (e.g., 1 / (1 + distance))
        proximity_score = 1 / (1 + dist_km)
        weighted_score = weight * proximity_score
        total_score += weighted_score

    # 3. Adjust for Time (Crucial!)
    # Students are more likely to be in certain places at certain times.
    hour = time.hour
    is_weekend = time.weekday() >= 5  # 5=Saturday, 6=Sunday

    time_multiplier = 1.0
    if 8 <= hour < 18 and not is_weekend:
        time_multiplier = 1.5  # Higher likelihood on campus during weekday class hours
    elif (18 <= hour < 23) and is_weekend:
        time_multiplier = (
            2.0  # Much higher likelihood in entertainment districts on weekend nights
        )
    elif 0 <= hour < 6:
        time_multiplier = 0.3  # Very low likelihood everywhere in the early morning

    total_score = total_score * time_multiplier

    # 4. Cap the score to a reasonable range (e.g., 0 to 3)
    return min(max(total_score, 0), 3)


# Pseudocode for an advanced version


def get_college_presence_score_advanced(cell_lat, cell_lon, time):
    # 1. Start with the base proximity score from Method 1
    base_score = get_college_presence_score(cell_lat, cell_lon, time)

    # 2. Check if there is a major event happening near this cell
    events = scrape_university_events(
        time
    )  # Your function to get events for the datetime
    event_boost = 0
    for event in events:
        if (
            calculate_distance(cell_lat, cell_lon, event.lat, event.lon) < 2
        ):  # Within 2 km
            event_boost += (
                event.attendance_score
            )  # e.g., a score you estimate based on the event type

    # 3. Check the academic calendar
    calendar_factor = get_academic_calendar_factor(
        time
    )  # returns 0.0 on break, 1.0 during normal class, 1.5 during finals

    # 4. Combine all factors
    total_score = (base_score + event_boost) * calendar_factor

    return total_score
