import googlemaps

G_MAPS_API_KEY = "AIzaSyDMzJpQk3yvLGho5yKDslrTEEPoAGDqYXo"
SQL_FOLDER_ID = '0B9Fc6ijLP56VbmRqQXV6WjJDeE0'
google_maps = googlemaps.Client(key=G_MAPS_API_KEY)


def get_geo_code(zip_code):
    """
    :param zip_code: zip code to search for lat on long
    :return: zip code and latitude and longitude in a list
    """

    global google_maps

    zip_string = str(zip_code)
    if len(zip_string) < 5:
        zip_string = '0' + zip_string
    fixed_zip = zip_string
    zip_string = zip_string + ' US'

    try:
        location = google_maps.geocode(zip_string)
        address_info = [fixed_zip, location[0]['geometry']['location']['lat'], location[0]['geometry']['location']['lng']]
        for component in location[0]['address_components']:
            address_info.append(component['long_name'])
        return address_info

    except:
        return 'No Result'
