from .env import X_SERVICE_TOKEN
from .functions import request_wrapper, chunker
import requests
import json


class ServiceApi:

    """
    
        We have some microservices in our api that we can use to get some data
        which can be better in javascript for performance reasons. This class
        is used to make requests to those microservices.
    
    """

    def __init__(self):
        self.base_url = 'https://prod-api.graphitti.io/api/service/v2'

    def headers(self):

        """
        
            Get the headers required to make requests to the service api.
        
        """

        return {
            'Content-Type': 'application/json',
            'x-service-token': X_SERVICE_TOKEN
        }
    
    @request_wrapper
    def _post(self, url, data = {}):

        """
        
            @param url(str): The url to make the POST request to
            @param data(dict): The data to send in the POST request
        
            This method is used to make a POST request to the service api.
        
        """

        # Make the POST request
        response = requests.post(url, headers=self.headers(), data=json.dumps(data))
        return response.json()

    def get_dominant_colors(self, image_urls):

        """

            @param image_urls(str[]): An array of image urls
        
            This method takes an array of image urls and returns an array of
            objects with the dominant colors of each image.

            Example:

                image_urls = [ 'https://www.example.com/image1.jpg', 'https://www.example.com/image2.jpg' ]

                response = [
                    {
                        'image_url': 'https://www.example.com/image1.jpg',
                        'dominant_color': '#ffffff'
                    },
                    {
                        'image_url': 'https://www.example.com/image2.jpg',
                        'dominant_color': '#000000'
                    }
                ]
        
        """
        
        # Define the URL for the POST request
        url = self.base_url + '/dominant-colors'

        # Define the JSON body for the POST request
        data = { 'image_urls': image_urls }

        # Our api can only handle so many of these at a time, so we need to
        # chunk the image urls into groups
        chunk_size = 100
        data = []
        chunks = chunker(image_urls, chunk_size)
        for chunk in chunks:
            data += self._post(url, { 'image_urls': chunk })

        return data
    
