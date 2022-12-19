from face_recog_config import EvaluationConfig
import sys
import os
from time import time
a_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(a_path)
from sklearn.datasets import fetch_lfw_pairs, fetch_lfw_people
from deepface import DeepFace
from face_req_handler import FaceRequestHandler, local_file_to_b64_img, r_cache
import atexit
import base64

from tqdm import tqdm
dirs = {
    "lfw": "lfw_funneled"
}

dataset_path = a_path + "/nld_face_recognition/dataset/" + dirs[EvaluationConfig.dataset]

logger = open(EvaluationConfig.OUTPUT_FPATH, 'w')


def log(*s):
    if s != "END":
        line = ""
        for arg in s:
            if type(arg) != str:
                arg = str(arg)
            line = line + arg
        line = line + "\n"
        logger.write(line)
        return
    else:
        logger.close()


def list_subdirs(path=dataset_path):
    return [d for d in os.listdir(path)]


def load_lfw():

    res = dict()
    cnt = 0
    for sd in list_subdirs():
        d = os.path.join(dataset_path, sd)
        if not os.path.isdir(d):
            continue
        files = list_subdirs(d)
        if len(files) == 1:
            continue

        cnt += 1
        res.update({sd:[os.path.join(d, f) for f in files]})
        if cnt >= EvaluationConfig.MAX_PERSON:
            break

    for person in res:
        f = res[person].pop()
        binary = local_file_to_b64_img(f)
        rep = FaceRequestHandler.return_representaion(binary, try_best=True)
        if rep is None:
            continue
        FaceRequestHandler.register_user_representation(person, rep)


    return res


def load_dataset():
    log("Dataset Loading")
    remain = load_lfw()
    log("Dataset Successfully loaded")
    log("loaded ", str(len(remain)),"People.")
    return load_lfw()

def exit_handler():
    log("Total amount of time for Face Recognition: ", Evaluation.total_time)
    log("Total_trial:", Evaluation.total_trial)
    log("Succeed : ", Evaluation.success)
    log("END")



class Evaluation:
    num_to_test = 0
    total_time = 0
    total_trial = 0
    success = 0
    @classmethod
    def evaluate(cls):
        log("Evaluation Start")
        FaceRequestHandler.handler_init("evaluation", model_name=EvaluationConfig.model)
        log("Model Loaded")
        targets = load_dataset()

        for person in targets:
            cls.num_to_test += len(targets[person])

        for person in targets:
            for tfiles in targets[person]:
                binary = local_file_to_b64_img(tfiles)
                cls.total_trial += 1
                toc = time()
                rep = FaceRequestHandler.specify_user(binary, try_best=True)
                tic = time()
                time_elapsed = tic-toc
                if rep is None:
                    log("Cannot specify user from a file of ", person, " ... File :", tfiles)

                else:
                    specified_name = rep.loc[0, 'person_id']
                    cls.total_time += time_elapsed
                    if specified_name == person:
                        cls.success += 1
                    log("source:",specified_name, "\t\ttarget:", person,"\t\t", "time taken:", time_elapsed)

                print("trial: ",cls.total_trial,"/",cls.num_to_test, "took ", time_elapsed)
        log("Evaluation ended")
        return



if __name__ == '__main__':
    atexit.register(exit_handler)
    result = []
    if EvaluationConfig.MAX_PERSON == 0:
        lfw = fetch_lfw_pairs(subset='test', color=True, resize=1)
        pairs = lfw.pairs
        targets = lfw.target
        model = DeepFace.build_model(EvaluationConfig.model)
        predictions = list()
        log("Experiment on built in dataset lfw, ")
        for idx in tqdm(range(0, pairs.shape[0])):
            pair = pairs[idx]
            img1 = pair[0]
            img2 = pair[1]
            actual = targets[idx]
            obj = DeepFace.verify(img1, img2, model_name=EvaluationConfig.model,
                                  model=model, enforce_detection=False,
                                  detector_backend="retinaface")
            prediction = 1 if obj['distance'] < 0.3 else 0
            if prediction == targets[idx]:
                log("trial ", idx, " success")
            else:
                log("trial ", idx, " fail")
            result.append(obj['distance'])
            predictions.append(prediction)
        from sklearn.metrics import classification_report
        import matplotlib.pyplot as plt
        from pandas import DataFrame
        from random import uniform
        same_person = result[:500]
        diff_person = result[500:]
        df = DataFrame({'axis1': [0 + uniform(-0.3, 0.3) for i in range(500)],
                        'axis2':[1+uniform(-0.3, 0.3) for i in range(500)],'same_person': same_person, 'diff_person': diff_person})
        plt.scatter(df['axis1'], df['same_person'])
        plt.scatter(df['axis2'], df['diff_person'])
        plt.savefig('output.png', dpi=300)



        log(classification_report(targets, predictions))
    else:
        Evaluation.evaluate()