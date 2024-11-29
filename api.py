from flask import Flask, jsonify, request,send_file
from backend.config.config import MetadataInputConfig
from frontend.eventsPrimaryLayout import EventsPrimaryLayout
'''Example usage is provided here:
http://localhost:8000/turbovault?metadataSource=Database
'''


app = Flask(__name__)
@app.route('/turbovault', methods = ['GET'])
def main():
    sections = {
    'SourcePlatform': 'Database',
    'SourceYML': True, 
    'Tasks': ['Stage', 'Standard Hub', 'Standard Satellite', 'Standard Link', 'Non-Historized Link',
            'Point-in-Time', 'Non-Historized Satellite', 'Multi-Active Satellite', 'Record Tracking Satellite'],
    'Sources': None,
    'DBDocs': False,
    'Properties': True,
    }
    events: object = EventsPrimaryLayout(
        config = MetadataInputConfig().data['config'], 
        validSourcePlatforms = sections['SourcePlatform'],
    )  
    sections['Sources']= events.onDropdownSelection(sections['SourcePlatform'])  
    events.onPressStart(sections)
    return 'all models for the source platform: {0} generated'.format(sections['Sources'])
    
if __name__ == '__main__':
    app.run(debug=True, port=8000)

## unit test
'''def main():
    sections = {
    'SourcePlatform': 'Database',
    'SourceYML': True, 
    'Tasks': ['Stage', 'Standard Hub', 'Standard Satellite', 'Standard Link', 'Non-Historized Link',
            'Point-in-Time', 'Non-Historized Satellite', 'Multi-Active Satellite', 'Record Tracking Satellite'],
    'Sources': None,
    'DBDocs': False,
    'Properties': True,
    }
    events: object = EventsPrimaryLayout(
        config = MetadataInputConfig().data['config'], 
        validSourcePlatforms = sections['SourcePlatform'],
    )  
    sections['Sources']= events.onDropdownSelection(sections['SourcePlatform'])  
    events.onPressStart(sections)
    
if __name__ == '__main__':
    main()'''