from tqdm import tqdm
import numpy as np
import pandas as pd
import os
import json

from keras.layers import Input, LSTM, Reshape
from keras.models import Model, Sequential
from keras.layers.core import Dense, Dropout
from keras.layers.advanced_activations import LeakyReLU
from keras.optimizers import Adam
from keras import initializers


def get_optimizer():
    return(Adam(lr=0.0002, beta_1=0.5))

def get_rgenerator(optimizer, lstm_cell, random_dim, dim_out):
    inp = Input(shape=(random_dim,1))
    x = LSTM(lstm_cell)(inp)
    relu = LeakyReLU(0.2)(x)
    gen = Dense(dim_out,activation='tanh')(relu)

    # Don't forget to reshape
    gen = Reshape((dim_out,1))(gen)
    rgen = Model(inputs=inp,outputs=gen)
    rgen.compile(loss='binary_crossentropy',optimizer=optimizer)
    return(rgen)

def get_rdiscriminator(optimizer, lstm_cell, dim_out):
    inp = Input(shape=(dim_out,1))

    x = LSTM(lstm_cell)(inp)
    relu = LeakyReLU(0.2)(x)
    drop = Dropout(0.3)(relu)

    disc = Dense(1,activation="sigmoid")(drop)

    rdisc = Model(inputs=inp,outputs=disc)
    rdisc.compile(loss='binary_crossentropy',optimizer=optimizer)
    return(rdisc)

# Get GAN
def get_gan_network(discriminator, generator, optimizer, random_dim):
    # We initially set trainable to False since we only want to train either the
    # generator or discriminator at a time

    discriminator.trainable = False
    gan_input = Input(shape=(random_dim,))
    x = generator(gan_input)
    gan_output = discriminator(x)
    gan = Model(inputs=gan_input, outputs=gan_output)
    gan.compile(loss='binary_crossentropy', optimizer=optimizer)
    return(gan)

def get_rgan_network(discriminator, generator, optimizer, random_dim):
    # We want to train either the discriminator or the generator so we start by puting the discriminator as
    # not trainable.

    discriminator.trainable = False
    # gan input will be the forger inspiration: 100 dim random noised vector
    gan_input = Input(shape=(random_dim,1))
    # The output of the generator is an image => 784 dim vector
    img = generator(gan_input)

    # Pass that image to the discriminator : is it a real image or not
    gan_output = discriminator(img)
    # Create model
    gan = Model(inputs = gan_input, outputs = gan_output)
    gan.compile(loss = "binary_crossentropy", optimizer = optimizer)
    return(gan)

def prepare_gan(optimizer, lstm_cell, random_dim, dim_out):
    generator = get_rgenerator(optimizer, lstm_cell, random_dim, dim_out)
    discriminator = get_rdiscriminator(optimizer, lstm_cell, dim_out)
    gan = get_rgan_network(discriminator, generator, optimizer, random_dim)
    return(generator, discriminator, gan)

# Save models
def save_generator(ticker, generator, path):
    partition_path = os.path.join(path, ticker)
    if not os.path.exists(partition_path):
        os.makedirs(partition_path)
    generator.save_weights(partition_path + "/generator.h5")
    #generator.save(partition_path + "/generator.h5")
    model_json = generator.to_json()
    with open(partition_path + "/generator.json", 'w') as f:
        json.dump(model_json, f)
    print("Model {0} saved on disk".format(ticker))
