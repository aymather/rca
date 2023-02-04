from .env import RAPID_API_KEY
import requests


class RapidApi:

    def getDadJoke(self):

        try:

            url = 'https://dad-jokes.p.rapidapi.com/random/joke'

            headers = {
                'X-RapidAPI-Key': RAPID_API_KEY,
                'X-RapidAPI-Host': 'dad-jokes.p.rapidapi.com'
            }

            response = requests.request('GET', url, headers=headers)
            response = response.json()

            setup = response['body'][0]['setup']
            punchline = response['body'][0]['punchline']

            return setup, punchline

        except:
            return 'Sorry.. I dont have a good one today..', 'That\'s no joke..'