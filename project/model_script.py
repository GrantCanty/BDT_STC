import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
import tensorflow as tf

# Load data
scaled_train = pd.read_csv("temp_scaled_train.csv")
train_df = pd.read_csv("temp_df.csv")
scaled_test = pd.read_csv("temp_scaled_test.csv")



target_cols = ['refuse', 'paper', 'mgp', 'res_organic', 'school_organic', 'leaves', 'xmas']

X_train_full = scaled_train.values
y_train_no_scale = train_df[target_cols].values

X_test_full = scaled_test.values
y_test_no_scale = test_df[target_cols].values

# Scale the target variables
y_scaler = StandardScaler()
y_train_reduced = y_scaler.fit_transform(y_train_no_scale)
y_test_reduced = y_scaler.transform(y_test_no_scale)

# Create sequences
def create_sequences(X, y, time_steps):
    X_seq, y_seq = [], []
    for i in range(len(X) - time_steps):
        X_seq.append(X[i:i+time_steps])  # Input sequence
        y_seq.append(y[i+time_steps])    # Target at future time step
    return np.array(X_seq), np.array(y_seq)

time_steps = 32
X_train, y_train = create_sequences(X_train_full, y_train_reduced, time_steps)
X_test, y_test = create_sequences(X_test_full, y_test_reduced, time_steps)

# Reshape data
X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], X_train.shape[2]))
X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], X_test.shape[2]))

# Reduce RAM usage
X_train = X_train.astype('float16')
y_train = y_train.astype('float16')
X_test = X_test.astype('float16')
y_test = y_test.astype('float16')

def fit_and_evaluate(model, X_train, y_train, X_test, y_test, learning_rate, epochs):
    early_stopping_cb = tf.keras.callbacks.EarlyStopping(
        monitor="val_mae", patience=50, restore_best_weights=True)

    opt = tf.keras.optimizers.Adam(learning_rate=learning_rate)

    model.compile(loss="mae", optimizer=opt, metrics=["mae"])

    history = model.fit(X_train, y_train, validation_data=(X_test, y_test), epochs=epochs,
                        callbacks=[early_stopping_cb])

    # Evaluate on test set
    valid_loss, valid_mae = model.evaluate(X_test, y_test)

    # Generate predictions
    predictions = model.predict(X_test)

    return valid_mae, predictions

# Define the model
target_cols_len = len(target_cols)
tf.random.set_seed(42)  # extra code – ensures reproducibility
lstm_model = tf.keras.models.Sequential([
    tf.keras.layers.LSTM(45, return_sequences=True, activation='tanh', input_shape=[X_train.shape[1], X_train.shape[2]]),
    tf.keras.layers.LSTM(15, return_sequences=False, activation='tanh'),
    tf.keras.layers.Dense(target_cols_len)
])

# Load model weights
lstm_model.load_weights("bdt_project/lstm_1.weights.h5")

# Evaluate the model
valid_mae, predictions = fit_and_evaluate(lstm_model, X_train, y_train, X_test, y_test, learning_rate=0.001, epochs=100)
