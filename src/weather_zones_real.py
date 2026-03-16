"""
Weather zones based on Open Power System Data renewable power plants
Generated from OPSD dataset analysis + manual additions
"""

WEATHER_ZONES = {
    'FR': {
        'wind_onshore': [
            {'lat': 44.096862, 'lon': 3.172104, 'weight': 0.118307, 'capacity_mw': 1756.5},
            {'lat': 49.683940, 'lon': 2.309902, 'weight': 0.349539, 'capacity_mw': 5189.7},
            {'lat': 47.476175, 'lon': -1.325177, 'weight': 0.216740, 'capacity_mw': 3218.0},
            {'lat': 48.483092, 'lon': 4.545661, 'weight': 0.315413, 'capacity_mw': 4683.0},
        ],
        'solar': [
            {'lat': 44.141179, 'lon': 0.809854, 'weight': 0.404482, 'capacity_mw': 2430.5},
            {'lat': 48.384997, 'lon': 4.970357, 'weight': 0.111242, 'capacity_mw': 668.4},
            {'lat': 44.024668, 'lon': 5.327155, 'weight': 0.318191, 'capacity_mw': 1912.0},
            {'lat': 47.222994, 'lon': -0.305684, 'weight': 0.166085, 'capacity_mw': 998.0},
        ],
        'hydro': [
            {'lat': 45.165323, 'lon': 5.813451, 'weight': 0.411433, 'capacity_mw': 821.6},
            {'lat': 43.493816, 'lon': 1.307601, 'weight': 0.378000, 'capacity_mw': 754.8},
            {'lat': 48.117453, 'lon': 5.964790, 'weight': 0.118201, 'capacity_mw': 236.0},
            {'lat': 46.809867, 'lon': 1.114121, 'weight': 0.092367, 'capacity_mw': 184.4},
        ],
        'biomass': [
            {'lat': 47.694722, 'lon': -0.955249, 'weight': 0.167917, 'capacity_mw': 129.1},
            {'lat': 47.655422, 'lon': 5.884187, 'weight': 0.142256, 'capacity_mw': 109.3},
            {'lat': 44.478230, 'lon': 3.415537, 'weight': 0.308508, 'capacity_mw': 237.1},
            {'lat': 49.372626, 'lon': 2.452534, 'weight': 0.381319, 'capacity_mw': 293.1},
        ],
        'wind_offshore': [
            {'lat': 49.500000, 'lon': 1.000000, 'weight': 0.600000, 'capacity_mw': 1000.0},
            {'lat': 47.500000, 'lon': -3.000000, 'weight': 0.400000, 'capacity_mw': 800.0},
        ],
        'load': [
            {'lat': 48.856600, 'lon': 2.352200, 'weight': 0.200000, 'capacity_mw': 0.0, 'name': 'Paris'},
            {'lat': 45.764000, 'lon': 4.835700, 'weight': 0.080000, 'capacity_mw': 0.0, 'name': 'Lyon'},
            {'lat': 43.296500, 'lon': 5.369800, 'weight': 0.050000, 'capacity_mw': 0.0, 'name': 'Marseille'},
            {'lat': 44.837800, 'lon': -0.579200, 'weight': 0.040000, 'capacity_mw': 0.0, 'name': 'Bordeaux'},
            {'lat': 47.218400, 'lon': -1.553600, 'weight': 0.030000, 'capacity_mw': 0.0, 'name': 'Nantes'},
        ],
    },
    'DE': {
        'wind_onshore': [
            {'lat': 53.325412, 'lon': 8.656238, 'weight': 0.315378, 'capacity_mw': 7342.7},
            {'lat': 50.386801, 'lon': 7.991584, 'weight': 0.241865, 'capacity_mw': 5631.1},
            {'lat': 51.536696, 'lon': 12.153659, 'weight': 0.260462, 'capacity_mw': 6064.1},
            {'lat': 53.375534, 'lon': 12.740255, 'weight': 0.182295, 'capacity_mw': 4244.2},
        ],
        'solar': [
            {'lat': 52.131225, 'lon': 12.762407, 'weight': 0.409112, 'capacity_mw': 6489.0},
            {'lat': 52.630220, 'lon': 8.305929, 'weight': 0.189727, 'capacity_mw': 3009.3},
            {'lat': 49.417033, 'lon': 8.510984, 'weight': 0.165924, 'capacity_mw': 2631.8},
            {'lat': 48.896160, 'lon': 11.415265, 'weight': 0.235237, 'capacity_mw': 3731.2},
        ],
        'hydro': [
            {'lat': 48.643767, 'lon': 8.917076, 'weight': 0.359071, 'capacity_mw': 461.9},
            {'lat': 48.340641, 'lon': 11.908537, 'weight': 0.259847, 'capacity_mw': 334.2},
            {'lat': 51.405405, 'lon': 8.777662, 'weight': 0.171467, 'capacity_mw': 220.6},
            {'lat': 50.958889, 'lon': 12.937988, 'weight': 0.209616, 'capacity_mw': 269.6},
        ],
        'biomass': [
            {'lat': 53.250953, 'lon': 9.825522, 'weight': 0.220117, 'capacity_mw': 1670.7},
            {'lat': 52.258237, 'lon': 12.744812, 'weight': 0.195067, 'capacity_mw': 1480.6},
            {'lat': 48.851568, 'lon': 10.719940, 'weight': 0.340039, 'capacity_mw': 2580.9},
            {'lat': 51.738895, 'lon': 7.796723, 'weight': 0.244777, 'capacity_mw': 1857.9},
        ],
        'wind_offshore': [
            {'lat': 54.500000, 'lon': 6.500000, 'weight': 0.400000, 'capacity_mw': 3000.0},
            {'lat': 54.700000, 'lon': 7.500000, 'weight': 0.400000, 'capacity_mw': 3000.0},
            {'lat': 54.800000, 'lon': 14.000000, 'weight': 0.200000, 'capacity_mw': 1500.0},
        ],
        'load': [
            {'lat': 52.520000, 'lon': 13.405000, 'weight': 0.150000, 'capacity_mw': 0.0, 'name': 'Berlin'},
            {'lat': 48.135100, 'lon': 11.582000, 'weight': 0.080000, 'capacity_mw': 0.0, 'name': 'Munich'},
            {'lat': 53.551100, 'lon': 9.993700, 'weight': 0.080000, 'capacity_mw': 0.0, 'name': 'Hamburg'},
            {'lat': 50.937500, 'lon': 6.960300, 'weight': 0.060000, 'capacity_mw': 0.0, 'name': 'Cologne'},
            {'lat': 50.110900, 'lon': 8.682100, 'weight': 0.040000, 'capacity_mw': 0.0, 'name': 'Frankfurt'},
        ],
    },
    'BE': {
        'wind_onshore': [
            {'lat': 50.800000, 'lon': 3.500000, 'weight': 0.300000, 'capacity_mw': 600.0, 'name': 'West Flanders'},
            {'lat': 51.100000, 'lon': 4.200000, 'weight': 0.400000, 'capacity_mw': 800.0, 'name': 'Antwerp Province'},
            {'lat': 50.500000, 'lon': 5.000000, 'weight': 0.200000, 'capacity_mw': 400.0, 'name': 'Brabant'},
            {'lat': 50.300000, 'lon': 5.800000, 'weight': 0.100000, 'capacity_mw': 200.0, 'name': 'Eastern Belgium'},
        ],
        'solar': [
            {'lat': 50.800000, 'lon': 4.300000, 'weight': 0.400000, 'capacity_mw': 1200.0, 'name': 'Central Belgium'},
            {'lat': 51.100000, 'lon': 4.800000, 'weight': 0.300000, 'capacity_mw': 900.0, 'name': 'Northern Belgium'},
            {'lat': 50.400000, 'lon': 4.000000, 'weight': 0.200000, 'capacity_mw': 600.0, 'name': 'Southern Belgium'},
            {'lat': 50.200000, 'lon': 5.500000, 'weight': 0.100000, 'capacity_mw': 300.0, 'name': 'Eastern Belgium'},
        ],
        'wind_offshore': [
            {'lat': 51.600000, 'lon': 2.800000, 'weight': 0.500000, 'capacity_mw': 1000.0, 'name': 'North Sea Zone 1'},
            {'lat': 51.400000, 'lon': 2.900000, 'weight': 0.500000, 'capacity_mw': 1000.0, 'name': 'North Sea Zone 2'},
        ],
        'hydro': [
            {'lat': 50.200000, 'lon': 5.800000, 'weight': 0.600000, 'capacity_mw': 200.0, 'name': 'Ardennes'},
            {'lat': 50.400000, 'lon': 4.500000, 'weight': 0.400000, 'capacity_mw': 100.0, 'name': 'Central Valleys'},
        ],
        'load': [
            {'lat': 50.850300, 'lon': 4.351700, 'weight': 0.350000, 'capacity_mw': 0.0, 'name': 'Brussels'},
            {'lat': 51.219400, 'lon': 4.402500, 'weight': 0.200000, 'capacity_mw': 0.0, 'name': 'Antwerp'},
            {'lat': 51.054300, 'lon': 3.717400, 'weight': 0.150000, 'capacity_mw': 0.0, 'name': 'Ghent'},
            {'lat': 50.632600, 'lon': 5.579700, 'weight': 0.100000, 'capacity_mw': 0.0, 'name': 'Liege'},
        ],
    },
}
