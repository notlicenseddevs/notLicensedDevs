import sys
import os
a_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(a_path)

os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
os.environ["CUDA_VISIBLE_DEVICES"] = "0"

from deepface.commons import distance as dst
from deepface import DeepFace
import base64

from nld_face_recognition.face_recog_config import FaceRecognitionConfig as FConfig

from nld_server.mongo_manager import MongoConnection
from pandas import DataFrame
from time import time
# hardly referenced deepface opensource
# Citation:"./Citation.txt"

B64_IMG_PREFIX = "data:image/,"

r_cache = DataFrame({'mongo_id' : [], 'representation':[]})

_debug = FConfig.DEBUG
def local_file_to_b64_img(img_path):
    # for testing purpose...
    with open(img_path, "rb") as fp:
        b = fp.read()
    return base64.b64encode(b).decode('ascii')


class FaceRequestHandler:
    model_names = None
    model_name = None
    metric_names = None
    mode = None
    initialized = False
    @classmethod
    def handler_init(cls, loading_mode='practical', model_name=FConfig.model_name):
        global r_cache
        print("Face Handler Initializing...")
        cls.model_name = model_name
        DeepFace.build_model(cls.model_name)
        if cls.model_name == 'Ensemble':
            cls.model_names = ['VGG-Face', 'Facenet', 'OpenFace', 'DeepFace']
            cls.metric_names = ['cosine', 'euclidean', 'euclidean_l2']
        elif cls.model_name != 'Ensemble':
            cls.model_names = [cls.model_name]
            cls.metric_names = [FConfig.distance_metric]
        if loading_mode not in ['practical', 'evaluation']:
            raise ValueError('Initialize failure, loading mode must be \'practical\' or \'evaluation\'')
        cls.mode = loading_mode
        if cls.mode == "evaluation":
            global r_cache
            r_cache = DataFrame({'person_id' : [], 'representation':[]})
            cls.initialized = True
            return

        # test version
        _tmp = list()
        mongo_client = MongoConnection()

        db_name = "nld"

        col_name = 'user'
        _tmp = list()
        for doc in mongo_client.client[db_name][col_name].find():
            if doc['user_face_representation'] is None:
                continue
            else:
                if type(doc['user_face_representation']) == str:
                    doc['user_face_representation'] = cls.return_representaion(doc['user_face_representation'])
                    mongo_client[db_name][col_name].find_one_and_update(
                        {'_id':doc['_id']},
                        {'$set':{'user_face_representation':doc['user_face_representation']}}
                    )
                _tmp.append({'mongo_id':doc['_id'], 'representation' : doc['user_face_representation']})

        r_cache = r_cache.append(_tmp, ignore_index=True)


        if _debug:
            print("Warming Up..")
        b = local_file_to_b64_img(a_path+'/nld_face_recognition/input_img/warmup.jpg')
        cls.return_representaion(b, try_best=True)
        if _debug:
            print("Done")

        cls.initialized = True
        print("Face Handler Initialized!!!")


        return cls.initialized


    @classmethod
    def return_representaion(cls, b64_img, try_best=False, enforce_detection=True):
        deepface_input = 'data:image/,' + b64_img

        if _debug:
            print("Getting Representation with detector backend = ", FConfig.detector_backend, " model name = ",
                cls.model_name)

        try:
            _ret = DeepFace.represent(deepface_input,
                                 detector_backend=FConfig.detector_backend
                                    )
        except ValueError:
            if _debug:
                print("Face Could not be found")
            if not try_best:
                return None
            _ret = None
            backends = ['opencv', 'ssd', 'dlib', 'mtcnn', 'retinaface', 'mediapipe']
            backends.remove(FConfig.detector_backend)
            for b in backends:
                if _debug:
                    print("Retry representation with detector backend of ",b)
                try:
                    _tmp = DeepFace.represent(deepface_input, detector_backend=b)
                    if _tmp:
                        _ret = _tmp
                        break
                except ValueError:
                    continue

        if _ret is None and not enforce_detection:
            _ret = DeepFace.represent(deepface_input,
                                      detector_backend=FConfig.detector_backend, enforce_detection=enforce_detection)
        return _ret

    @classmethod
    def register_user_representation(cls, mongo_oid, representation):
        assert cls.initialized

        if _debug:
            print("Register user's representation in memory")

        if cls.mode == "practical":
            idx = 'mongo_id'
        else:
            idx = "person_id"


        global r_cache
        if mongo_oid in r_cache:
            r_cache.loc[r_cache[idx] == mongo_oid, 'representation'] = representation
        else:
            r_cache = r_cache.append({idx:mongo_oid, 'representation': representation}, ignore_index=True)
        return True

    @classmethod
    def specify_user(cls, b64_img, return_time=False, try_best=False):
        assert cls.initialized


        if return_time:
            tic = time()

        model_name = cls.model_names[0]
        df = r_cache.copy()

        tic = time()
        target_rep = cls.return_representaion(b64_img, try_best=try_best)

        if target_rep is None:
            return None
        if _debug:
            print("Comparing with db's representations")
        for m in cls.metric_names:
            distances = []
            for index, instance in df.iterrows():
                source_rep = instance['representation']
                if source_rep is None:
                    distances.append(float('inf'))
                    continue
                distance = None
                if m == 'cosine':
                    distance = dst.findCosineDistance(source_rep, target_rep)
                elif m == 'euclidean':
                    distance = dst.findEuclideanDistance(source_rep, target_rep)
                elif m == 'euclidean_l2':
                    distance = dst.findEuclideanDistance(dst.l2_normalize(source_rep),
                                                         dst.l2_normalize(target_rep))
                distances.append(distance)

            # if model_name == 'Ensemble' and j == 'OpenFace' and k == 'euclidean':
            # continue

            df['%s_%s' % (model_name, m)] = distances
            if model_name != 'Ensemble':
                threshold = dst.findThreshold(model_name, m)
                df = df.drop(columns=['representation'])
                df = df[df['%s_%s' % (model_name, m)] <= threshold]

                df = df.sort_values(by=['%s_%s' % (model_name, m)], ascending=True).reset_index(drop=True)



        if df.empty:
            _result = None
        else:
            _result = df
        if return_time:
            return _result, time() - tic
        else:
            return _result
