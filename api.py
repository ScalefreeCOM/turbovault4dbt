from flask import Flask, jsonify, request,send_file
from backend.config.config import MetadataInputConfig
from frontend.eventsPrimaryLayout import EventsPrimaryLayout
app = Flask()
@app.route('/turbovault', method = ['GET'])
def main():
    config : object = MetadataInputConfig().data
    name = request.args.get('metadataSource')
    events: object = EventsPrimaryLayout(
        config = config, 
        validSourcePlatforms = name,
    )    
    events.onPressStart(name)
    
if __name__ == '__main__':
    app.run(debug=True, port=8000)