from tensorflow.keras.layers import Dense, LSTM
from tensorflow.keras.models import Sequential

def get_model():
    xmodel = Sequential()
    xmodel.add(LSTM(128, inputshape = (20, 1)), return_sequences= True)
    xmodel.add(LSTM(128, return_sequences = True))
    xmodel.add(LSTM(128, return_sequences = False))
    xmodel.add(Dense(1))

    return xmodel
