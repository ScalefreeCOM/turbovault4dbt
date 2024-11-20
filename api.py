from flask import Flask, jsonify, request,send_file
from backend.config.config import MetadataInputConfig
from frontend.eventsPrimaryLayout import EventsPrimaryLayout
'''app = Flask(__name__)
@app.route('/turbovault', methods = ['GET'])
def main():
    config : object = MetadataInputConfig().data
    name = request.args.get('metadataSource')
    events: object = EventsPrimaryLayout(
        config = config, 
        validSourcePlatforms = name,
    )    
    events.onPressStart(name)
    
if __name__ == '__main__':
    app.run(debug=True, port=8000)'''


def main():
    name = 'Excel'
    events: object = EventsPrimaryLayout(
        config = MetadataInputConfig().data['config'], 
        validSourcePlatforms = name,
    )    
    events.onPressStart(name)
    
if __name__ == '__main__':
    main()