from src_rest.loaders.data_loaders_api import load_mosdata
from src_rest.transformers.transform import concat_data

if __name__ == '__main__':
    print('Launching...')
    concat_data()
    print('Ending...')
