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

    def getDominantColors(self, image_urls):

        """
        
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

        # Define the headers for the POST request
        headers = {
            'Content-Type': 'application/json',
            'x-service-token': 'oN8TB0T93C7ZgZG2LldsDiF2'
        }

        # Define the JSON body for the POST request
        data = {
            'image_urls': image_urls
        }

        # Make the POST request
        response = requests.post(url, headers=headers, data=json.dumps(data))
        return response.json()