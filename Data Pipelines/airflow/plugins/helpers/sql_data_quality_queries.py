
class SqlDataQualityQueries(object):
    
    queries = {
        "staging_events": {
            "test": "SELECT COUNT(*) FROM {}",
            "not_expected": 0
        },
        
        "staging_songs": {
            "test": "SELECT COUNT(*) FROM {}",
            "not_expected": 0
        },
        
        "time": {
            "test": "SELECT COUNT(*) FROM {} WHERE start_time IS NULL",
            "expected": 0
        },
        
        "songs": {
            "test": "SELECT COUNT(*) FROM {} WHERE songid IS NULL",
            "expected": 0
        },
        
        "songplays": {
            "test": "SELECT COUNT(*) FROM {} WHERE playid IS NULL",
            "expected": 0
        },
        
        "artists": {
            "test": "SELECT COUNT(*) FROM {} WHERE artistid IS NULL",
            "expected": 0
        }
        
    }
    