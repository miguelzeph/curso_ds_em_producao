import pandas as pd
import json
import requests
import os
from flask import Flask, Response, request

# Constants
TOKEN = str( os.environ['TOKEN'] )# Pega no bot do Telegram


# --------------------------Como usar bot Telegram ------------------------------

# info about the Bot
#url ='https://api.telegram.org/bot'+TOKEN+'/getMe'

# get updates
#url ='https://api.telegram.org/bot'+TOKEN+'/getUpdates'

# send message
#url ='https://api.telegram.org/bot'+TOKEN+'/sendMessage?chat_id=883145506&text=Hi Miguel'

# webhook (para que o telegram encontre sua máquina... tem que chegar a mensagem aqui no python)
# Então você vai na url do Browser e copia esse código, para set o telegram a enviar as mensagens para a máquina no heroku.
#url ='https://api.telegram.org/bot'+TOKEN+'/setWebhook?url=LINK_DO_HEROKU'

#----------------------------my functions------------------------------------------


def send_message( chat_id, text ):
    url = 'https://api.telegram.org/bot{}/'.format( str(TOKEN) ) 
    url = url + 'sendMessage?chat_id={}'.format( chat_id ) 

    r = requests.post( url, json={'text': text } )
    print( 'Status Code {}'.format( r.status_code ) )

    return None

def send_photo( chat_id, photo ):
    url = 'https://api.telegram.org/bot{}/'.format( str(TOKEN) ) 
    url = url + 'sendPhoto?chat_id={}'.format( chat_id ) 

    r = requests.post( url, json={'photo': photo } )
    print( 'Status Code {}'.format( r.status_code ) )

    return None


def load_dataset( store_id ):
    # loading test dataset
    df10 = pd.read_csv( 'https://raw.githubusercontent.com/miguelzeph/curso_ds_em_producao/master/data/test.csv',low_memory=False )
    df_store_raw = pd.read_csv( 'https://raw.githubusercontent.com/miguelzeph/curso_ds_em_producao/master/data/store.csv',low_memory=False )

    # merge test dataset + store
    df_test = pd.merge( df10, df_store_raw, how='left', on='Store' )

    # choose store for prediction
    df_test = df_test[df_test['Store'] == store_id]

    if not df_test.empty:
        # remove closed days
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop( 'Id', axis=1 )

        # convert Dataframe to json
        data = json.dumps( df_test.to_dict( orient='records' ) )

    else:
        data = 'error'

    return data


def predict( data ):
    # API Call
    #url = 'http://0.0.0.0:5000/rossmann/predict' # LOCAL
    url = 'https://dsproducao2021.herokuapp.com/rossmann/predict' # Heroku
    header = {'Content-type': 'application/json' }
    data = data

    r = requests.post( url, data=data, headers=header )
    print( 'Status Code {}'.format( r.status_code ) )

    d1 = pd.DataFrame( r.json(), columns=r.json()[0].keys() )

    return d1


def parse_message( message ):

        

    chat_id = message['message']['chat']['id']

    if message['message']['text'] in ['start','/start','Start','/Start']:
        store_id = 'start'
        return chat_id, store_id

    store_id = message['message']['text']
    store_id = store_id.replace( '/', '' )


    try:
        store_id = int( store_id )

    except ValueError:
        store_id = 'error'

    return chat_id, store_id


# API initialize
app = Flask( __name__ )


@app.route( '/', methods=['GET', 'POST'] )
def index():
    if request.method == 'POST':
        message = request.get_json()

        chat_id, store_id = parse_message( message )
	
        if store_id != 'error' and store_id !='start' :
            # loading data
            data = load_dataset( store_id )

            if data != 'error':
                # prediction
                d1 = predict( data )
                # calculation
                d2 = d1[['store', 'prediction']].groupby( 'store' ).sum().reset_index()
                
                # send message
                msg = 'Store Number {} will sell R${:,.2f} in the next 6 weeks'.format(
                            d2['store'].values[0],
                            d2['prediction'].values[0] ) 

                send_message( chat_id, msg )
                return Response( 'Ok', status=200 )

            else:
                send_message( chat_id, 'Store Not Available, try other store number.' )
                return Response( 'Ok', status=200 )
        
        elif store_id == 'start':
            send_message( chat_id, 
            'Instructions: program developed by Miguel to make sales predictions in the next 6 weeks of all Rossmann stores (datas provided by Kaggle). Each store has an ID number ... to see its prediction, Enter a number:')
            #send_photo( chat_id, 'https://github.com/miguelzeph/curso_ds_em_producao/blob/telegram_bot/img/teste.png?raw=true')
            send_photo( chat_id, 'img/teste.png')
            return Response( 'Ok', status=200 )

        else:
            send_message( chat_id, 'Store ID is Wrong, please type a number.' )
            return Response( 'Ok', status=200 )


    else:
        return '<h1> Rossmann Telegram BOT </h1>'


if __name__ == '__main__':
    port = os.environ.get( 'PORT', 5000 )
    app.run( host='0.0.0.0', port=port )



