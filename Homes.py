import os
from concurrent.futures import ThreadPoolExecutor
import requests
import json
import folium
from folium.plugins import HeatMap
import branca.colormap as cm

MAX_THREAD = 500

# The geographic coordinates of Wellington, New Zealand, for specifying the map range
WELLINGTON_POLYLINE = "nifyFw`}h`@?sl_BvjxA??rl_BwjxA?"
# Uncomment the desired WELLINGTON_POLYLINE below for other regions in New Zealand:
# NORTH_NZ_POLYLINE = "j{ptEwlgu_@?sizj@x}}g@??rizj@y}}g@?"
# ALL_NZ_POLYLINE = "njafEseaw]?okyxCres|A??nkyxCses|A?"

# The data to be sent in the POST request to the server for fetching property information
SEARCH_PARAMS = {
    "polylines": [WELLINGTON_POLYLINE],
    "limit": 6000,
    "display_rentals": False,
    "for_rent": False,
    "for_sale": True,
    "just_sold": False,
    "off_market": False
}


class Homes:
    def __init__(self):
        self.homes_data = []  # Final processed data for each home
        self.homes_data_raw = []  # Raw data fetched from server for each home
        self.homes_for_sale_urls = []  # URLs of homes for sale
        self.total_home_count = None  # Total count of homes found
        self.dots = None  # JSON data containing home information
        self.save_file = 'all.json'  # File to save the final data
        # URLs for making requests to the server
        self.post_url = 'https://gateway.homes.co.nz/map/dots'
        self.get_url = 'https://gateway.homes.co.nz/property?url='
        self.details_url = 'https://gateway.homes.co.nz/listing/'
        self.property_url = 'https://gateway.homes.co.nz/property/'
        self.export_url = 'https://homes.co.nz/address'

    # Fetches data from the server to get the geographic coordinates of properties
    def get_dots(self):
        response = requests.post(self.post_url, data=json.dumps(SEARCH_PARAMS))

        try:
            self.dots = response.json()["map_items"]
            self.total_home_count = len(self.dots)
            print(f"Found {self.total_home_count} homes.")
        except KeyError:
            print("The set limit exceeds the webpage's hosting capacity. Please lower the limit.")

    # Extracts URLs of homes for sale from the fetched data
    def get_homes_urls(self):
        for i in range(len(self.dots)):
            home = self.dots[i]["url"]
            self.homes_for_sale_urls.append(home)

    # Collects data for each home using multithreading for faster processing
    def collect_homes_data(self):
        thread_homes = []
        dot_count = len(self.dots)
        with ThreadPoolExecutor(max_workers=min(MAX_THREAD, dot_count)) as executor:
            for i in range(dot_count):
                thread_home = executor.submit(self.get_home_data, self.dots[i])
                print(f"Start threading {i + 1}/{dot_count}")
                thread_homes.append(thread_home)

            for thread_home in thread_homes:
                self.homes_data_raw.append(thread_home.result())

    # Cleans and processes the raw home data into a more structured format
    def clean_homes_data(self):
        for home_data in self.homes_data_raw:
            home_data_dict = {}
            try:
                home_detail = home_data['home_detail']
            except KeyError as e:
                continue
            property_details = home_data['home_detail']['property_details']

            # Extract property details
            home_data_dict['address'] = property_details['address']
            home_data_dict['num_bathrooms'] = property_details['num_bathrooms']
            home_data_dict['num_bedrooms'] = property_details['num_bedrooms']
            home_data_dict['num_car_spaces'] = property_details['num_car_spaces']
            home_data_dict['num_bathrooms'] = property_details['num_bathrooms']

            home_data_dict['sales_count'] = home_detail['sales_count']
            home_data_dict['listing_images'] = property_details['listing_images']
            home_data_dict['headline'] = property_details['headline']
            home_data_dict['estimated_value_revision_date'] = property_details['estimated_value_revision_date']
            home_data_dict['display_estimated_lower_value_short'] = property_details[
                'display_estimated_lower_value_short']
            home_data_dict['display_estimated_upper_value_short'] = property_details[
                'display_estimated_upper_value_short']
            home_data_dict['display_estimated_value_short'] = property_details['display_estimated_value_short']
            home_data_dict['estimated_rental_revision_date'] = property_details['estimated_rental_revision_date']
            home_data_dict['display_estimated_rental_lower_value_short'] = property_details[
                'display_estimated_rental_lower_value_short']
            home_data_dict['display_estimated_rental_upper_value_short'] = property_details[
                'display_estimated_rental_upper_value_short']
            home_data_dict['estimated_rental_yield'] = property_details['estimated_rental_yield']
            home_data_dict['capital_value'] = property_details['capital_value']
            home_data_dict['improvement_value'] = property_details['improvement_value']
            home_data_dict['land_value'] = property_details['land_value']
            home_data_dict['current_revision_date'] = property_details['current_revision_date']
            home_data_dict['city_id'] = property_details['city_id']
            home_data_dict['suburb_id'] = property_details['suburb_id']
            home_data_dict['unit_identifier'] = property_details['unit_identifier']
            home_data_dict['street_number'] = property_details['street_number']
            home_data_dict['street_alpha'] = property_details['street_alpha']
            home_data_dict['street'] = property_details['street']
            home_data_dict['suburb'] = property_details['suburb']
            home_data_dict['city'] = property_details['city']
            home_data_dict['coordinates'] = home_detail['point']
            home_data_dict['display_price'] = home_detail['display_price']
            home_data_dict['listed_date'] = home_detail['date']
            home_data_dict['url'] = 'https://homes.co.nz/address' + home_detail['url']
            home_data_dict['solar'] = home_detail['solar']

            # Extract agent details if available
            if 'agent' in home_detail:
                if home_detail['agent']:  # sometimes home_detail['agent'] is None
                    home_data_dict['agent_name'] = home_detail['agent']['name']
                    home_data_dict['agent_office_phone'] = home_detail['agent']['office_phone']
                    home_data_dict['agent_mobile_phone'] = home_detail['agent']['mobile_phone']
                    home_data_dict['agent_sale_stats'] = home_detail['agent']['sale_stats']

            # Extract listing details if available
            if 'listing_detail' in home_data:
                listing_detail = home_data['listing_detail']
                home_data_dict['listing_type'] = listing_detail['listing_type']
                home_data_dict['property_type'] = listing_detail['property_type']
                home_data_dict['headline'] = listing_detail['headline']
                home_data_dict['description'] = listing_detail['description']
                home_data_dict['num_bedrooms'] = listing_detail['num_bedrooms']
                home_data_dict['num_bathrooms'] = listing_detail['num_bathrooms']
                home_data_dict['num_car_spaces'] = listing_detail['num_car_spaces']
                home_data_dict['floor_area_m2'] = listing_detail['floor_area_m2']
                home_data_dict['land_area_m2'] = listing_detail['land_area_m2']
                home_data_dict['pets'] = listing_detail['pets']
                home_data_dict['smokers'] = listing_detail['smokers']
                home_data_dict['furnishings'] = listing_detail['furnishings']
                home_data_dict['max_tenants'] = listing_detail['max_tenants']
                home_data_dict['cover_image_url'] = listing_detail['cover_image_url']
                home_data_dict['media'] = listing_detail['media']
                home_data_dict['status'] = listing_detail['status']
                home_data_dict['authority'] = listing_detail['authority']
                home_data_dict['open_homes'] = listing_detail['open_homes']
                home_data_dict['created_at'] = listing_detail['created_at']
                home_data_dict['updated_at'] = listing_detail['updated_at']
                home_data_dict['parking_description'] = listing_detail['parking_description']

            self.homes_data.append(home_data_dict)

    # Writes the processed data to a JSON file
    def write_to_file(self):
        with open(self.save_file, 'w') as file:
            file.write(json.dumps(self.homes_data))

    # Reads data from the JSON file
    def read_data_from_file(self):
        with open(self.save_file, 'r') as file:
            json_str = file.read()
            self.homes_data = json.loads(json_str)

    # Fetches data from the server and processes it to a file
    def fetch_data_to_file(self):
        self.get_dots()
        self.get_homes_urls()
        self.collect_homes_data()
        self.clean_homes_data()

    # Fetches detailed data for a specific home using its URL
    def get_home_data(self, home):
        result = {}

        try:
            home_detail_url = f"{self.get_url}{home['url']}"
            with requests.Session() as session:
                home_detail = session.get(home_detail_url)
                home_detail.raise_for_status()
                item_id = home_detail.json()['card']['item_id']
                listing_url = f"{self.details_url}{item_id}/detail"
                listing_details = session.get(listing_url)
                listing_details.raise_for_status()
                result['home_detail'] = home_detail.json()['card']
                if 'listing' in listing_details.json():
                    result['listing_detail'] = listing_details.json()['listing']
        except requests.RequestException as e:
            print(f"Error occurred during the request: {e}")

        return result


class HomeHeatMapGenerator:
    def __init__(self, latitude, longitude, zoom_level=11):
        self.latitude = latitude
        self.longitude = longitude
        self.zoom_level = zoom_level
        self.heat_map = folium.Map(location=[latitude, longitude], zoom_start=zoom_level)

    # Generates a heatmap based on the input data and value_str
    def generate_heatmap(self, data, value_str):
        value_list = [item[2] for item in data]
        try:
            colormap = cm.linear.YlGnBu_09.scale(min(value_list), max(value_list))
        except ValueError as e:
            print(f"Error occurred while creating colormap: {e}")
            colormap = cm.linear.YlGnBu_09.scale(0, 1)

        HeatMap(data).add_to(self.heat_map)

        for i in range(len(data)):
            lat, lon, value = data[i]
            color = colormap(value)  # Get color based on the weight value
            folium.Circle(location=[lat, lon], radius=5, color=color, fill=True, fill_color=color,
                          tooltip=value_str[i]).add_to(self.heat_map)

    # Displays the generated heatmap on the map
    def display_map(self):
        file_path = 'heatmap.html'
        self.heat_map.save(file_path)
        print("Opening heatmap...")
        os.system(f'start {file_path}')


class Utility:
    def __init__(self):
        pass

    @staticmethod
    def money_string_to_float(money_str):
        unit = money_str[-1].upper()
        multiplier = 1

        if unit == 'K':
            multiplier = 1e3
        elif unit == 'M':
            multiplier = 1e6

        return float(money_str[:-1]) * multiplier

    # Extracts heat map data from the homes_data
    @staticmethod
    def get_heat_map_data(data):
        map_data = []
        map_value_str = []
        for i in data:
            estimated_value = i['display_estimated_value_short']
            if estimated_value != 'TBC':
                map_value_str.append(estimated_value)
                coordinates = i['coordinates']
                estimated_value = Utility.money_string_to_float(estimated_value) * 0.0001
                map_data.append([coordinates['lat'], coordinates['long'], estimated_value])

        latitude = sum(item[0] for item in map_data) / len(map_data)
        longitude = sum(item[1] for item in map_data) / len(map_data)
        return latitude, longitude, map_data, map_value_str


if __name__ == '__main__':
    homes = Homes()
    homes.fetch_data_to_file()
    homes.write_to_file()
    homes.read_data_from_file()

    input_latitude, input_longitude, input_data, tooltips = Utility.get_heat_map_data(homes.homes_data)

    heatmap_generator = HomeHeatMapGenerator(input_latitude, input_longitude)
    heatmap_generator.generate_heatmap(input_data, tooltips)
    heatmap_generator.display_map()
