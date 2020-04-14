from flask import Flask
from flask import jsonify
import pandas as pd
import json
app = Flask(__name__)

df_raw = pd.read_csv("/home/runqi/Workspace/Spring2020/CS591K1/challenge/dataset2/in1.csv", header=None)

current_idx = 0
chunk_size=1000
finish = False

def get_chunk():
    global finish
    global current_idx
    global chunk_size
    if finish:
        return None
    if current_idx + chunk_size>len(df_raw):
        df_chunk = df_raw.iloc[current_idx:len(df_raw),:]
        finish=True
    else:
        df_chunk = df_raw.iloc[current_idx:current_idx+chunk_size,:]
        current_idx = current_idx+chunk_size
    return df_chunk

def transfer_dict(df_chunk):
    result = []
    for _,each in df_chunk.iterrows():
        temp = {}
        temp['i'] = int(each[0])
        temp['voltage'] = each[1]
        temp['current'] = each[2]
        result.append(temp)
    return result



@app.route('/data/1/', methods=["GET"])
def hello_world():
    df_chunk = get_chunk()
    if df_chunk is None:
        return jsonify({"records": []}), 403
    else:
        return jsonify({"records": transfer_dict(df_chunk)})

if __name__ == '__main__':
      app.run(host='0.0.0.0', port=80)
