import gc
gc.disable()
import os
import sys
import datetime
import time
import pickle
import base64
import numpy as np
import json

from ctypes import cdll
import ctypes

client = None
FUNC = ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_char_p)
FUNC2 = ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_int)

#to server
OP_RECV                      = 0x00
#OP_CLIENT_WAKE_UP            = 0x01 #obsolete
OP_CLIENT_READY              = 0x02
OP_CLIENT_UPDATE             = 0x03
OP_CLIENT_EVAL               = 0x04
#to client
OP_INIT                      = 0x05
OP_REQUEST_UPDATE            = 0x06
OP_STOP_AND_EVAL             = 0x07

def storeData(fname, data):
    # Its important to use binary mode
    datafile = open(fname, 'ab')
    # source, destination
    pickle.dump(data, datafile)
    datafile.close()

def loadData(fname):
    datafile = open(fname, 'rb')
    db = pickle.load(datafile)
    datafile.close()
    return db

def obj_to_pickle_string(x):
    return base64.b64encode(pickle.dumps(x))

def pickle_string_to_obj(s):
    #return pickle.loads(base64.b64decode(s))
    return pickle.loads(base64.b64decode(s, '-_'))

class LocalModel(object):
    def __init__(self, model_config, data_collected):
        # model_config:
            # 'model': self.local_model.model.to_json(),
            # 'model_id'
            # 'min_train_size'
            # 'data_split': (0.6, 0.3, 0.1), # train, test, valid
            # 'epoch_per_round'
            # 'batch_size'

        # for convergence check
        self.prev_train_loss = None

        # all rounds; losses[i] = [round#, timestamp, loss]
        # round# could be None if not applicable
        self.train_losses = []
        self.valid_losses = []
        self.train_accuracies = []
        self.valid_accuracies = []

        self.model_config = model_config
        self.model_id = model_config['model_id']

        from keras.models import model_from_json
        self.model = model_from_json(model_config['model_json'])
        # the weights will be initialized on first pull from server

        self.client_no = 0
        self.max_clients = 2

        self.li = [0] * self.max_clients
        self.li[self.client_no] = 1

        train_data, test_data, valid_data = data_collected
        print("data collected")
        print((train_data[0]).shape)
        # print(train_data.size)
        # print(test_data.size)
        # print(valid_data.size)
        self.x_train = []
        # for iter,tup in enumerate(train_data):
        #     self.x_train.append(tup)
        # print(f"round {iter}")
        # self.x_train = np.array(self.x_train)
        self.x_train = np.array([tup for tup in train_data])
        self.y_train = np.array([self.li for tup in train_data])
        self.x_test = np.array([tup for tup in test_data])
        self.y_test = np.array([self.li for tup in test_data])
        self.x_valid = np.array([tup for tup in valid_data])
        self.y_valid = np.array([self.li for tup in valid_data])

        print(self.x_train.shape)
        print(self.y_train.shape)
        print(self.x_test.shape)
        print(self.y_test.shape)
        print(self.x_valid.shape)
        print(self.y_valid.shape)

        #for lower clients
        self.current_weights = self.model.get_weights()

        self.training_start_time = int(round(time.time()))

    def get_weights(self):
        return self.model.get_weights()

    def set_weights(self, new_weights):
        self.model.set_weights(new_weights)

    # return final weights, train loss, train accuracy
    def train_one_round(self):
        import tensorflow.keras as keras
        opt = keras.optimizers.Adam(learning_rate=0.000001)
        self.model.compile(loss=keras.losses.categorical_crossentropy,
            optimizer=opt,
            metrics=['accuracy'])

        # print(self.x_valid)
        # print(self.y_valid)
        print(self.x_valid.shape)
        print(self.y_valid.shape)
        print("x n y valid")

        # print(self.x_train)
        # print(self.y_train)
        print(self.x_train.shape)
        print(self.y_train.shape)
        print("x n y train")

        self.model.summary()
        score = self.model.evaluate(self.x_valid[0:20], self.y_valid[0:20], verbose=1)
        print('Train loss before training:', score[0])
        print('Train accuracy before training:', score[1])
        csv_file = open(f"csv_file{self.client_no}.csv", 'a')
        csv_file.write(f"o{score[1]}"+'\n')
        csv_file.close()
        # print(self.y_train)
        print(self.y_train[0])
        from keras.callbacks import CSVLogger

        csv_logger = CSVLogger(f'csv_file{self.client_no}.csv', append=True, separator=',')
        x = np.roll(self.x_train, 8, axis=0)
        y = np.roll(self.y_train, 8, axis=0)

        from keras.callbacks import ReduceLROnPlateau
        lr_reduce = ReduceLROnPlateau(monitor='val_accuracy', factor=0.6, patience=8, verbose=1, mode='max', min_lr=5e-5)


        self.model.fit(self.x_train[0:8], self.y_train[0:8],
                epochs=1,
                batch_size=2,
                verbose=1,
                validation_data=(self.x_valid[0:20], self.y_valid[0:20]),callbacks=[csv_logger,lr_reduce])

        score = self.model.evaluate(self.x_valid[0:20], self.y_valid[0:20], verbose=1)
        csv_file = open(f"csv_file{self.client_no}.csv", 'a')
        csv_file.write(f"o{score[1]}"+'\n')
        csv_file.close()
        print('Train loss:', score[0])
        print('Train accuracy:', score[1])
        return self.model.get_weights(), score[0], score[1]

    def validate(self):
        score = self.model.evaluate(self.x_valid, self.y_valid, verbose=0)

        print('Validate loss:', score[0])
        print('Validate accuracy:', score[1])
        return score

    def evaluate(self):
        score = self.model.evaluate(self.x_test, self.y_test, verbose=0)
        print('Test loss:', score[0])
        print('Test accuracy:', score[1])
        return score

    #for lower clients
    def update_weights(self, client_weights, client_sizes):
        new_weights = [np.zeros(w.shape) for w in self.current_weights]
        total_size = np.sum(client_sizes)

        #for w in self.current_weights:
        #    print(w.shape)

        for c in range(len(client_weights)):
            for i in range(len(new_weights)):
                new_weights[i] += client_weights[c][i] * client_sizes[c] / total_size
        self.current_weights = new_weights

    def aggregate_loss_accuracy(self, client_losses, client_accuracies, client_sizes):
        total_size = np.sum(client_sizes)
        # weighted sum
        aggr_loss = np.sum(client_losses[i] / total_size * client_sizes[i]
                for i in range(len(client_sizes)))
        aggr_accuraries = np.sum(client_accuracies[i] / total_size * client_sizes[i]
                for i in range(len(client_sizes)))
        return aggr_loss, aggr_accuraries, total_size

    # cur_round coule be None
    def aggregate_train_loss_accuracy(self, client_losses, client_accuracies, client_sizes, cur_round):
        cur_time = int(round(time.time())) - self.training_start_time
        aggr_loss, aggr_accuraries, aggr_size = self.aggregate_loss_accuracy(client_losses, client_accuracies, client_sizes)

        print(cur_round)
        print(cur_time)
        print(aggr_loss)
        self.train_losses += [[cur_round, cur_time, aggr_loss]]
        self.train_accuracies += [[cur_round, cur_time, aggr_accuraries]]
        with open('stats.txt', 'w') as outfile:
            json.dump(self.get_stats(), outfile)
        return aggr_loss, aggr_accuraries

    # cur_round coule be None
    def aggregate_valid_loss_accuracy(self, client_losses, client_accuracies, client_sizes, cur_round):
        cur_time = int(round(time.time())) - self.training_start_time
        aggr_loss, aggr_accuraries, aggr_size = self.aggregate_loss_accuracy(client_losses, client_accuracies, client_sizes)
        self.valid_losses += [[cur_round, cur_time, aggr_loss]]
        self.valid_accuracies += [[cur_round, cur_time, aggr_accuraries]]
        with open('stats.txt', 'w') as outfile:
            json.dump(self.get_stats(), outfile)
        return aggr_loss, aggr_accuraries

    def get_stats(self):
        return {
            "train_loss": self.train_losses,
            "valid_loss": self.valid_losses,
            "train_accuracy": self.train_accuracies,
            "valid_accuracy": self.valid_accuracies
        }

class FederatedClient(object):
    MIN_NUM_WORKERS = 0 #total from this branch. This will be set by grouping protocol during grouping
    MAX_NUM_ROUNDS = 1
    ROUNDS_BETWEEN_VALIDATIONS = 2
    MAX_DATASET_SIZE_KEPT = 1200
    #def __init__(self, host, port, bootaddr, datasource):
    def __init__(self, host, port, bootaddr, datasource):
        self.local_model = None
        self.datasource = datasource()
       
        self.current_round = 0
        self.current_round_client_updates = []
        self.eval_client_updates = []
 
        print("p2p init")
        self.lib = cdll.LoadLibrary('./GRING_plugin.so')
        self.lib.Init_p2p.restype = ctypes.c_char_p
        self.lib.Fedcomp_GR.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_byte]
        self.lib.Report_GR.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_byte, ctypes.c_int]

        self.register_handles()
        self.lib.Init_p2p(host.encode('utf-8'),int(port), int(0), bootaddr.encode('utf-8'))

        self.lib.Bootstrapping(bootaddr.encode('utf-8'))


    def register_handles(self):

        def on_set_num_client(num):
            print('APP : on set_num_client')
            self.MIN_NUM_WORKERS = num
            print('APP : set MIN_NUM_WORKERS ',self.MIN_NUM_WORKERS)

        def on_init_subleader(data):
            print('APP : on init_subleader')
            model_config = pickle_string_to_obj(data)

            # generate local data
            if os.path.exists("fake_data") and os.path.exists("my_class_distr"):
                fake_data = loadData("fake_data")
                my_class_distr = loadData("my_class_distr")
            else:
                fake_data, my_class_distr = self.datasource.fake_non_iid_data(
                    min_train=model_config['min_train_size'],
                    max_train=FederatedClient.MAX_DATASET_SIZE_KEPT,
                    data_split=model_config['data_split']
                )
                #storeData("fake_data",fake_data)
                #storeData("my_class_distr",my_class_distr)

            self.local_model = LocalModel(model_config, fake_data)

            self.lib.IncreaseNumClientReady()
            
        def on_init_worker(data):
            print('APP : on init_worker')
            model_config = pickle_string_to_obj(data)

            # generate local data
            if os.path.exists("fake_data") and os.path.exists("my_class_distr"):
                fake_data = loadData("fake_data")
                my_class_distr = loadData("my_class_distr")
            else:
                fake_data, my_class_distr = self.datasource.fake_non_iid_data(
                    min_train=model_config['min_train_size'],
                    max_train=FederatedClient.MAX_DATASET_SIZE_KEPT,
                    data_split=model_config['data_split']
                )
                #storeData("fake_data",fake_data)
                #storeData("my_class_distr",my_class_distr)

            self.local_model = LocalModel(model_config, fake_data)

            print("send client_ready to upper leader\n")
            self.lib.Report_GR(None, 0, OP_CLIENT_READY, 1)

        # handler for initiator role
        def on_global_model(data):
            print('APP : on global model')
            model_config = pickle_string_to_obj(data)
            print(model_config)

            # generate local data
            if os.path.exists("fake_data") and os.path.exists("my_class_distr"):
                fake_data = loadData("fake_data")
                my_class_distr = loadData("my_class_distr")
            else:
                fake_data, my_class_distr = self.datasource.fake_non_iid_data(
                    min_train=model_config['min_train_size'],
                    max_train=FederatedClient.MAX_DATASET_SIZE_KEPT,
                    data_split=model_config['data_split']
                )
                #storeData("fake_data",fake_data)
                #storeData("my_class_distr",my_class_distr)

            self.local_model = LocalModel(model_config, fake_data)

            self.lib.IncreaseNumClientReady()

            metadata = {
                'model_json': self.local_model.model.to_json(), #since my model is updated with global model
                'model_id': model_config['model_id'],
                'min_train_size': model_config['min_train_size'],
                'data_split': model_config['data_split'],
                'epoch_per_round': model_config['epoch_per_round'],
                'batch_size': model_config['batch_size'],
            }
            sdata = obj_to_pickle_string(metadata)
            self.lib.Fedcomp_GR(sdata, sys.getsizeof(sdata),OP_INIT)


        def on_train_my_model(arg):
            # train my model
            start = datetime.datetime.now()

            self.local_model.current_weights, train_loss, train_accuracy = self.local_model.train_one_round()

            self.lib.IncreaseNumClientUpdateInitiator()

            end = datetime.datetime.now()

            diff = end - start
            #diff = datetime.timedelta(minutes=13, seconds=34)
            print("diff(sec) : " + str(diff.seconds)+str("\n"))
            self.lib.RecordMyTrainTime(diff.seconds)

        #subleader handler
        def on_client_update_subleader(data):
            print('APP : on client_update_subleader \n')
            data = pickle_string_to_obj(data)

            # gather updates and discard outdated update
            if data['round_number'] == self.current_round:
                self.current_round_client_updates += [data]
                self.current_round_client_updates[-1]['weights'] = pickle_string_to_obj(data['weights'])

        #initiator handler
        def on_client_update_initiator(data):
            print('on client_update_initiator\n')
            data = pickle_string_to_obj(data)
            #filehandle = open("run.log", "a")
            #filehandle.write ('on client_update: datasize :' + str(sys.getsizeof(data))+'\n')

            #filehandle.write("handle client_update", request.sid)
            #for x in data:
            #    if x != 'weights':
            #        print(x, data[x])
                    #filehandle.write (str(x)+ " " +str(data[x]) + '\n')
            #filehandle.close()

            # gather updates from members and discard outdated update
            if data['round_number'] == self.current_round:
                self.current_round_client_updates += [data]
                self.current_round_client_updates[-1]['weights'] = pickle_string_to_obj(data['weights'])

        def on_client_update_done_initiator(arg):
            print('on client_update_done_initiator\n')
            self.local_model.update_weights(
                [x['weights'] for x in self.current_round_client_updates],
                [x['train_size'] for x in self.current_round_client_updates],
            )
            aggr_train_loss, aggr_train_accuracy = self.local_model.aggregate_train_loss_accuracy(
                [x['train_loss'] for x in self.current_round_client_updates],
                [x['train_accuracy'] for x in self.current_round_client_updates],
                [x['train_size'] for x in self.current_round_client_updates],
                self.current_round
            )
            #filehandle = open("run.log", "a")
            #filehandle.write("aggr_train_loss"+str(aggr_train_loss)+'\n')
            #filehandle.write("aggr_train_accuracy"+str(aggr_train_accuracy)+'\n')
            #filehandle.close()

            if 'valid_loss' in self.current_round_client_updates[0]:
                aggr_valid_loss, aggr_valid_accuracy = self.local_model.aggregate_valid_loss_accuracy(
                [x['valid_loss'] for x in self.current_round_client_updates],
                [x['valid_accuracy'] for x in self.current_round_client_updates],
                [x['valid_size'] for x in self.current_round_client_updates],
                self.current_round
                )
                #filehandle = open("run.log", "a")
                #filehandle.write("aggr_valid_loss"+str(aggr_valid_loss)+'\n')
                #filehandle.write("aggr_valid_accuracy"+str(aggr_valid_accuracy)+'\n')
                #filehandle.close()

	    #TODO : this comment is for test. remove later. we need to stop when it converges.
            #if self.local_model.prev_train_loss is not None and \
            #        (self.local_model.prev_train_loss - aggr_train_loss) / self.local_model.prev_train_loss < .01:
            #    # converges
            #    filehandle = open("run.log", "a")
            #    filehandle.write("converges! starting test phase..")
            #    filehandle.close()
            #    self.stop_and_eval()
            #    return
            #self.local_model.prev_train_loss = aggr_train_loss

            if self.current_round >= FederatedClient.MAX_NUM_ROUNDS:
                # report to publisher. send the aggregated weight
                resp = {
                    'round_number': self.current_round,
                    'weights': obj_to_pickle_string(self.local_model.current_weights),
                    'train_size': self.local_model.x_train.shape[0],
                    'valid_size': self.local_model.x_valid.shape[0],
                    'train_loss': aggr_train_loss,
                    'train_accuracy': aggr_train_accuracy,
                }

                sresp = obj_to_pickle_string(resp)
                print('send CLIENT_UPDATE to server, msg payload size:' + str(sys.getsizeof(sresp)) + '\n' )
                self.lib.Report_GR(sresp, sys.getsizeof(sresp), OP_CLIENT_UPDATE, 0)

                # eval my model
                self.stop_and_eval()
                test_loss, test_accuracy = self.local_model.evaluate()
                resp = {
                    'test_size': self.local_model.x_test.shape[0],
                    'test_loss': test_loss,
                    'test_accuracy': test_accuracy
                }
                self.eval_client_updates += [resp]
                self.lib.IncreaseNumClientEvalInitiator()
            else:
                # report to publisher. send the aggregated weight
                resp = {
                    'round_number': self.current_round,
                    'weights': obj_to_pickle_string(self.local_model.current_weights),
                    'train_size': self.local_model.x_train.shape[0],
                    'valid_size': self.local_model.x_valid.shape[0],
                    'train_loss': aggr_train_loss,
                    'train_accuracy': aggr_train_accuracy,
                }
                sresp = obj_to_pickle_string(resp)
                print('send CLIENT_UPDATE to server, msg payload size:' + str(sys.getsizeof(sresp)) + '\n' )
                self.lib.Report_GR(sresp, sys.getsizeof(sresp), OP_CLIENT_UPDATE, 0)

                # send request updates to the members
                self.train_next_round()

                start = datetime.datetime.now()

                # train my model
                self.local_model.current_weights, train_loss, train_accuracy = self.local_model.train_one_round()

                # increase update done counter
                self.lib.IncreaseNumClientUpdateInitiator()

                end = datetime.datetime.now()
                diff = end - start
                print("diff(sec) : " + str(diff.seconds)+str("\n"))
                self.lib.RecordMyTrainTime(diff.seconds)

        # subleader handler
        def on_request_update_subleader(data):
            data = pickle_string_to_obj(data)
            print('APP : on request_update \n')

            self.current_round_client_updates = []

            self.current_round = data['round_number']
            print("round_number : "+str(data['round_number'])+"\n")

            weights = pickle_string_to_obj(data['current_weights'])

            #filehandle = open("run.log", "a")
            #filehandle.write ('on request_update received data size :' +str(sys.getsizeof(args)) + '\n')
            start = datetime.datetime.now()
            #filehandle.writelines("start : " + str(start)+str("\n"))
            #filehandle.close()

            # train my model
            self.local_model.set_weights(weights)
            self.local_model.current_weights, train_loss, train_accuracy = self.local_model.train_one_round()

            self.lib.IncreaseNumClientUpdate()

            end = datetime.datetime.now()
            diff = end - start
            print("diff(sec) : " + str(diff.seconds)+str("\n"))
            self.lib.RecordMyTrainTime(diff.seconds)

            resp = {
                'round_number': data['round_number'],
                'weights': self.local_model.current_weights,
                'train_size': self.local_model.x_train.shape[0],
                'valid_size': self.local_model.x_valid.shape[0],
                'train_loss': train_loss,
                'train_accuracy': train_accuracy,
            }
            #filehandle = open("run.log", "a")
            #filehandle.write ('train_loss' + str(train_loss) + '\n' )
            #filehandle.write ('train_accuracy' + str(train_accuracy) + '\n' )
            if data['run_validation']:
                valid_loss, valid_accuracy = self.local_model.validate()
                resp['valid_loss'] = valid_loss
                resp['valid_accuracy'] = valid_accuracy
                #filehandle.write ('valid_loss' + str(valid_loss) + '\n' )
                #filehandle.write ('valid_accuracy' + str(valid_accuracy) + '\n' )
            #filehandle.close()

            self.current_round_client_updates += [resp]


        # worker handler
        def on_request_update_worker(data):
            print('APP : on request_update_worker\n')
            data = pickle_string_to_obj(data)

            self.current_round = data['round_number']
            print("round_number : "+str(data['round_number'])+"\n")

            weights = pickle_string_to_obj(data['current_weights'])

            #filehandle = open("run.log", "a")
            #filehandle.write ('on request_update received data size :' +str(sys.getsizeof(args)) + '\n')
            #start = datetime.datetime.now()
            #filehandle.writelines("start : " + str(start)+str("\n"))
            #filehandle.close()

            start = datetime.datetime.now()

            self.local_model.set_weights(weights)
            self.local_model.current_weights, train_loss, train_accuracy = self.local_model.train_one_round()

            end = datetime.datetime.now()

            diff = end - start
            print("diff(sec) : " + str(diff.seconds)+str("\n"))
            self.lib.RecordMyTrainTime(diff.seconds)

            #filehandle = open("run.log", "a")
            #filehandle.writelines("end : " + str(end)+str("\n"))
            #filehandle.writelines("diff(s) : " + str(diff.seconds)+str("\n"))
            #filehandle.writelines("diff(us) : " + str(diff.microseconds)+str("\n"))
            #filehandle.close()

            resp = {
                'round_number': data['round_number'],
                'weights': obj_to_pickle_string(self.local_model.current_weights),
                'train_size': self.local_model.x_train.shape[0],
                'valid_size': self.local_model.x_valid.shape[0],
                'train_loss': train_loss,
                'train_accuracy': train_accuracy,
            }
            #filehandle = open("run.log", "a")
            #filehandle.write ('train_loss' + str(train_loss) + '\n' )
            #filehandle.write ('train_accuracy' + str(train_accuracy) + '\n' )
            if data['run_validation']:
                valid_loss, valid_accuracy = self.local_model.validate()
                resp['valid_loss'] = valid_loss
                resp['valid_accuracy'] = valid_accuracy
                #filehandle.write ('valid_loss' + str(valid_loss) + '\n' )
                #filehandle.write ('valid_accuracy' + str(valid_accuracy) + '\n' )
            #filehandle.close()

            sresp = obj_to_pickle_string(resp)
            print('send CLIENT_UPDATE to upper leader\n' )
            self.lib.Report_GR(sresp, sys.getsizeof(sresp), OP_CLIENT_UPDATE, 1)

        # sub-leader handler
        def on_stop_and_eval_subleader(data):
            data = pickle_string_to_obj(data)
            print('APP : on stop_and_eval_subleader')
            #filehandle = open("run.log", "a")
            #filehandle.write ('on stop_and_eval received data size :' +str(sys.getsizeof(args)) + '\n')

            #filehandle.write ('send CLIENT_EVAL to size:' + str(sys.getsizeof(sresp)) + '\n' )
            #filehandle.close()

            weights = pickle_string_to_obj(data['current_weights'])
            self.local_model.set_weights(weights)
            test_loss, test_accuracy = self.local_model.evaluate()

            resp = {
                'test_size': self.local_model.x_test.shape[0],
                'test_loss': test_loss,
                'test_accuracy': test_accuracy
            }

            self.eval_client_updates += [resp]

            self.lib.IncreaseNumClientEval()

        # worker handler
        def on_stop_and_eval_worker(data):
            print('APP : on stop_and_eval')
            data = pickle_string_to_obj(data)
            #filehandle = open("run.log", "a")
            #filehandle.write ('on stop_and_eval received data size :' +str(sys.getsizeof(args)) + '\n')
            weights = pickle_string_to_obj(data['current_weights'])

            self.local_model.set_weights(weights)
            test_loss, test_accuracy = self.local_model.evaluate()
            resp = {
                'test_size': self.local_model.x_test.shape[0],
                'test_loss': test_loss,
                'test_accuracy': test_accuracy
            }
            #filehandle.write ('send CLIENT_EVAL size:' + str(sys.getsizeof(sresp)) + '\n' )
            #filehandle.close()
            sdata = obj_to_pickle_string(resp)
            print('APP : on stop_and_eval: report')
            self.lib.Report_GR(sdata, sys.getsizeof(sdata), OP_CLIENT_EVAL, 1)

        def on_client_eval_subleader(data):
            data = pickle_string_to_obj(data)
            print ('APP : on client_eval_subleader\n')

            if self.eval_client_updates is None:
                return

            self.eval_client_updates += [data]

        #initiator handler
        def on_client_eval_initiator(data):
            data = pickle_string_to_obj(data)
            print ('APP : on client_eval\n')

            if self.eval_client_updates is None:
                return

            self.eval_client_updates += [data]

        def on_client_eval_done_initiator(arg):
            aggr_test_loss, aggr_test_accuracy, aggr_test_size = self.local_model.aggregate_loss_accuracy(
            [x['test_loss'] for x in self.eval_client_updates],
            [x['test_accuracy'] for x in self.eval_client_updates],
            [x['test_size'] for x in self.eval_client_updates],
            );
            filehandle = open("run.log", "a")
            filehandle.write("\nfinal aggr_test_loss"+str(aggr_test_loss)+'\n')
            filehandle.write("final aggr_test_accuracy"+str(aggr_test_accuracy)+'\n')
            filehandle.write("== done ==\n")
            print("== done ==\n")
            print("\nfinal aggr_test_loss"+str(aggr_test_loss)+'\n')
            print("final aggr_test_accuracy"+str(aggr_test_accuracy)+'\n')
            #self.end = int(round(time.time()))
            #filehandle.write("end : " + str(self.end)+'\n')
            #print("end : " + str(self.end)+'\n')
            #filehandle.write("diff : " + str(self.end - self.start)+'\n')
            #print("diff : " + str(self.end - self.start)+'\n')
            #filehandle.write("== done ==\n")
            #filehandle.close()
            #self.eval_client_updates = None  # special value, forbid evaling again

            #report to publisher
            resp = {
                'test_size': aggr_test_size,
                'test_loss': aggr_test_loss,
                'test_accuracy': aggr_test_accuracy
            }
            sdata = obj_to_pickle_string(resp)
            self.lib.Report_GR(sdata, sys.getsizeof(sdata), OP_CLIENT_EVAL, 0)

        def on_report_client_update(aggregation_num):
            print( "APP : report client update\n") 
            self.local_model.update_weights(
                [x['weights'] for x in self.current_round_client_updates],
                [x['train_size'] for x in self.current_round_client_updates],
            )
            aggr_train_loss, aggr_train_accuracy = self.local_model.aggregate_train_loss_accuracy(
                [x['train_loss'] for x in self.current_round_client_updates],
                [x['train_accuracy'] for x in self.current_round_client_updates],
                [x['train_size'] for x in self.current_round_client_updates],
                self.current_round
            )

            resp = {
                'round_number': self.current_round,
                'weights': obj_to_pickle_string(self.local_model.current_weights),
                'train_size': self.local_model.x_train.shape[0],
                'valid_size': self.local_model.x_valid.shape[0],
                'train_loss': aggr_train_loss,
                'train_accuracy': aggr_train_accuracy,
            }

            if 'valid_loss' in self.current_round_client_updates[0]:
                aggr_valid_loss, aggr_valid_accuracy = self.local_model.aggregate_valid_loss_accuracy(
                [x['valid_loss'] for x in self.current_round_client_updates],
                [x['valid_accuracy'] for x in self.current_round_client_updates],
                [x['valid_size'] for x in self.current_round_client_updates],
                self.current_round
                )
                resp['valid_loss'] = aggr_valid_loss
                resp['valid_accuracy'] = aggr_valid_accuracy
 
            sresp = obj_to_pickle_string(resp)
            print('send CLIENT_UPDATE to server, msg payload size:' + str(sys.getsizeof(sresp)) + '\n' )
            self.lib.Report_GR(sresp, sys.getsizeof(sresp), OP_CLIENT_UPDATE, aggregation_num)

        def on_train_next_round(arg):
            self.current_round += 1
            # buffers all client updates
            self.current_round_client_updates = []

            #filehandle = open("run.log", "a")
            #filehandle.write("### Round "+str(self.current_round)+"###\n")
            print("### Round "+str(self.current_round)+"###\n")
            #filehandle.close()

            metadata = {
                'model_id': self.local_model.model_id,
                'round_number': self.current_round,
                'current_weights': obj_to_pickle_string(self.local_model.current_weights),
                'run_validation': self.current_round % FederatedClient.ROUNDS_BETWEEN_VALIDATIONS == 0
            }
            sdata = obj_to_pickle_string(metadata)
            self.lib.Fedcomp_GR(sdata, sys.getsizeof(sdata), OP_REQUEST_UPDATE)
            print("request_update sent\n")

        def on_report_client_eval(aggregation_num):
            aggr_test_loss, aggr_test_accuracy, aggr_test_size = self.local_model.aggregate_loss_accuracy(
                [x['test_loss'] for x in self.eval_client_updates],
                [x['test_accuracy'] for x in self.eval_client_updates],
                [x['test_size'] for x in self.eval_client_updates],
            );
            self.eval_client_updates = None  # special value, forbid evaling again
            resp = {
                'test_size': aggr_test_size,
                'test_loss': aggr_test_loss,
                'test_accuracy': aggr_test_accuracy
            }
            #filehandle.write ('send CLIENT_EVAL size:' + str(sys.getsizeof(sresp)) + '\n' )
            #filehandle.close()
            sdata = obj_to_pickle_string(resp)
            self.lib.Report_GR(sdata, sys.getsizeof(sdata), OP_CLIENT_EVAL, aggregation_num)

        global onsetnumclient
        onsetnumclient = FUNC2(on_set_num_client)
        fnname="on_set_num_client"
        self.lib.Register_callback(fnname.encode('utf-8'),onsetnumclient)

        global onglobalmodel
        onglobalmodel = FUNC(on_global_model)
        fnname="on_global_model"
        self.lib.Register_callback(fnname.encode('utf-8'),onglobalmodel)

        global oninitworker
        oninitworker = FUNC(on_init_worker)
        fnname="on_init_worker"
        self.lib.Register_callback(fnname.encode('utf-8'),oninitworker)

        global oninitsubleader
        oninitsubleader = FUNC(on_init_subleader)
        fnname="on_init_subleader"
        self.lib.Register_callback(fnname.encode('utf-8'),oninitsubleader)

        global onrequestupdateworker
        onrequestupdateworker = FUNC(on_request_update_worker)
        fnname="on_request_update_worker"
        self.lib.Register_callback(fnname.encode('utf-8'),onrequestupdateworker)

        global onrequestupdatesubleader
        onrequestupdatesubleader = FUNC(on_request_update_subleader)
        fnname="on_request_update_subleader"
        self.lib.Register_callback(fnname.encode('utf-8'),onrequestupdatesubleader)

        global onstopandevalworker
        onstopandevalworker = FUNC(on_stop_and_eval_worker)
        fnname="on_stop_and_eval_worker"
        self.lib.Register_callback(fnname.encode('utf-8'),onstopandevalworker)

        global onstopandevalsubleader
        onstopandevalsubleader = FUNC(on_stop_and_eval_subleader)
        fnname="on_stop_and_eval_subleader"
        self.lib.Register_callback(fnname.encode('utf-8'),onstopandevalsubleader)

        global onclientupdatesubleader
        onclientupdatesubleader = FUNC(on_client_update_subleader)
        fnname="on_clientupdate_subleader"
        self.lib.Register_callback(fnname.encode('utf-8'),onclientupdatesubleader)

        global onclientupdateinitiator
        onclientupdateinitiator = FUNC(on_client_update_initiator)
        fnname="on_clientupdate_initiator"
        self.lib.Register_callback(fnname.encode('utf-8'),onclientupdateinitiator)

        global onclientupdatedoneinitiator
        onclientupdatedoneinitiator = FUNC(on_client_update_done_initiator)
        fnname="on_clientupdatedone_initiator"
        self.lib.Register_callback(fnname.encode('utf-8'),onclientupdatedoneinitiator)

        global onclientevalsubleader
        onclientevalsubleader = FUNC(on_client_eval_subleader)
        fnname="on_clienteval_subleader"
        self.lib.Register_callback(fnname.encode('utf-8'),onclientevalsubleader)

        global onclientevalinitiator
        onclientevalinitiator = FUNC(on_client_eval_initiator)
        fnname="on_clienteval_initiator"
        self.lib.Register_callback(fnname.encode('utf-8'),onclientevalinitiator)

        global onclientevaldoneinitiator
        onclientevaldoneinitiator = FUNC(on_client_eval_done_initiator)
        fnname="on_clientevaldone_initiator"
        self.lib.Register_callback(fnname.encode('utf-8'),onclientevaldoneinitiator)

        global onreportclientupdate
        onreportclientupdate = FUNC2(on_report_client_update)
        fnname="on_report_client_update"
        self.lib.Register_callback(fnname.encode('utf-8'),onreportclientupdate)

        global ontrainnextround
        ontrainnextround = FUNC(on_train_next_round)
        fnname="on_train_next_round"
        self.lib.Register_callback(fnname.encode('utf-8'),ontrainnextround)

        global onreportclienteval
        onreportclienteval = FUNC2(on_report_client_eval)
        fnname="on_report_client_eval"
        self.lib.Register_callback(fnname.encode('utf-8'),onreportclienteval)

        global ontrainmymodel
        ontrainmymodel = FUNC(on_train_my_model)
        fnname="on_train_my_model"
        self.lib.Register_callback(fnname.encode('utf-8'),ontrainmymodel)


    #internal function
    # Note: we assume that during training the #workers will be >= MIN_NUM_WORKERS
    def train_next_round(self):
        self.current_round += 1
        # buffers all client updates
        self.current_round_client_updates = []

        #filehandle = open("run.log", "a")
        #filehandle.write("### Round "+str(self.current_ro(und)+"###\n")
        print("### Round "+str(self.current_round)+"###\n")
        #filehandle.close()

        metadata = {
            'model_id': self.local_model.model_id,
            'round_number': self.current_round,
            'current_weights': obj_to_pickle_string(self.local_model.current_weights),
            'run_validation': self.current_round % FederatedClient.ROUNDS_BETWEEN_VALIDATIONS == 0
        }
        sdata = obj_to_pickle_string(metadata)
        self.lib.Fedcomp_GR(sdata, sys.getsizeof(sdata), OP_REQUEST_UPDATE)
        print("request_update sent\n")

    def stop_and_eval(self):
        self.eval_client_updates = []
        metadata = {
            'model_id': self.local_model.model_id,
            'current_weights': obj_to_pickle_string(self.local_model.current_weights)
        }
        sdata = obj_to_pickle_string(metadata)
        self.lib.Fedcomp_GR(sdata, sys.getsizeof(sdata), OP_STOP_AND_EVAL)

#global client
if __name__ == "__main__":
    filehandle = open("run.log", "w")
    filehandle.write("running client \n")
    filehandle.close()
    import datasource

    # for debug
    client = FederatedClient(sys.argv[1], sys.argv[2], sys.argv[3], datasource.VggFace2)

    #client = FederatedClient(sys.argv[1], sys.argv[2], sys.argv[3], datasource.VggFace2)

    # If you use run.sh to launch many peer nodes, you should comment below line
    #client.lib.Input()
    # Instead use this to block the process
    while True:
        pass
    
