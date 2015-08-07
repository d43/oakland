def crime_dict(df):
    d = {}
    for col in df.CrimeCat.unique()[1:]:
        if col.find("ASSAULT") == 0:
            d[col] = "VIOLENT"
        elif col.find("ROBBERY") == 0:
            d[col] = "VIOLENT"
        elif col.find("KIDNAPPING") == 0:
            d[col] = "VIOLENT"
        elif col.find("ARSON") == 0:
            d[col] = "VIOLENT"
        elif col.find("WEAPONS") == 0:
            d[col] = "VIOLENT"
        elif col.find("HOMICIDE") == 0:
            d[col] = "VIOLENT"
        elif col.find("SEX_RAPE") == 0:
            d[col] = "VIOLENT"
        elif col.find("SEX_OTHER") == 0:
            d[col] = "VIOLENT"
        elif col.find("LARCENY_THEFT_VEHICLE") == 0:
            d[col] = "VEHICLE_THEFT"
        elif col.find("OTHER_RECOVERED") == 0:
            d[col] = "VEHICLE_THEFT"
        elif col.find("LARCENY_BURGLARY_AUTO") == 0:
            d[col] = "VEHICLE_BREAK_IN"
        elif col.find("LARCENY") == 0:
            d[col] = "NONVIOLENT"
        elif col.find("SEX_PROSTITUTION") == 0:
            d[col] = "QUALITY"
        elif col.find("VANDALISM") == 0:
            d[col] = "QUALITY"
        elif col.find("QUALITY") == 0:
            d[col] = "QUALITY"
        elif col.find("TRAFFIC_DUI") == 0:
            d[col] = "QUALITY"
        elif col.find("DOM-VIOL") == 0:
            d[col] = "DOM-VIOL"
        elif col.find("MENTAL-ILLNESS") == 0:
            d[col] = "OTHER"
        elif col.find("COURT") == 0:
            d[col] = "WARRANT"
        else: d[col] = col

    return d