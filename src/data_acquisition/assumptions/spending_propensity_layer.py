def get_week_of_month_factor(day):
    # This function takes a datetime object and returns which week of the month it is (1-4/5)
    first_week = 1.4  # Strong boost right after payday for many
    second_week = 0.9
    third_week = 0.8
    fourth_week = 1.1  # Slight uptick before the next pay cycle
    fifth_week = 1.0  # Some months have 5 weeks, handle it gracefully

    week_number = (day.day - 1) // 7 + 1

    if week_number == 1:
        return first_week
    elif week_number == 2:
        return second_week
    elif week_number == 3:
        return third_week
    elif week_number == 4:
        return fourth_week
    else:
        return fifth_week


def get_monthly_factor(month):  # month is 1-12
    # Example values based on common trends. Tweak these based on your data!
    monthly_factors = {
        1: 0.8,  # January - Post-holiday slump, resolutions to save money
        2: 0.9,  # February
        3: 0.95,  # March
        4: 1.0,  # April
        5: 1.0,  # May
        6: 1.1,  # June - Summer begins, vacations
        7: 1.1,  # July - Summer vacations
        8: 1.0,  # August
        9: 1.0,  # September
        10: 1.2,  # October - Start of holiday season buildup?
        11: 1.4,  # November - Black Friday, Cyber Monday
        12: 1.5,  # December - Peak holiday spending
    }
    return monthly_factors.get(month, 1.0)  # default to 1.0 if month is invalid


def get_holiday_factor(date):
    # Define a dictionary of major holidays and their impact WINDOW.
    # The value is a tuple: (day_of_impact, number_of_days_before, number_of_days_after, multiplier)
    holiday_rules = {
        "Christmas": (25, 12, 30, 25, 2.0),  # Big boost for the whole month
        "Black_Friday": (4, 11, 24, 4, 11, 26, 2.5),  # The Friday after Thanksgiving
        "Thanksgiving": (
            4,
            11,
            23,
            4,
            11,
            23,
            1.2,
        ),  # Day itself might be low spend (family dinner) but day after is huge
        "New_Years_Eve": (12, 31, 12, 31, 1.8),
        "Independence_Day": (7, 4, 7, 3, 7, 5, 1.5),  # Boost on the 3rd, 4th, and 5th
        "Labor_Day": (1, 9, 1, 9, 3, 9, 5, 1.3),  # Long weekend effect
        # Add more holidays (Easter, Mother's Day, etc.)
    }

    for holiday, rules in holiday_rules.items():
        # You would need logic here to check if the provided 'date'
        # falls within the window defined by the rules.
        # This requires a bit more code to implement the date comparison.
        # Pseudocode: if date is in range(rules.start, rules.end): return rules.multiplier
        pass

    return 1.0  # Not near any holiday, no effect


# A simpler alternative for prototyping:
def get_holiday_factor_simple(date):
    simple_holidays = {
        (12, 25): 2.0,  # Christmas Day
        (11, 24): 2.5,  # Black Friday 2023
        (7, 4): 1.7,  # July 4th
        (12, 31): 1.8,  # New Year's Eve
        # ... add more (month, day) tuples
    }
    return simple_holidays.get((date.month, date.day), 1.0)


def get_dow_factor(weekday):  # 0=Monday, 6=Sunday
    dow_factors = [
        0.7,
        0.8,
        0.9,
        1.0,
        1.5,
        2.0,
        1.2,
    ]  # Mon, Tue, Wed, Thu, Fri, Sat, Sun
    return dow_factors[weekday]


def get_hour_factor(hour):  # 0-23
    # Simple curve: low at night, peaks in evening
    if 0 <= hour < 6:
        return 0.2
    elif 6 <= hour < 11:
        return 0.7
    elif 11 <= hour < 16:
        return 1.1
    elif 16 <= hour < 18:
        return 1.3
    else:  # 18 - 23
        return 1.8
