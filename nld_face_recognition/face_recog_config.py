
opensource_list = ["DeepFace", "InsightFace", "ageitgey"]


class FaceRecognitionConfig:
    DEBUG = True
    use_opensource = True
    if use_opensource:
        opensource_name = "DeepFace"
        detector_backend = "retinaface"
        model_name = "VGG-Face"
        distance_metric = "cosine"

    else:
        opensource_name = None

class EvaluationConfig:
    dataset = "lfw"
    model = "Facenet512"
    MAX_PERSON = 0 # 0 for the built in dataset from sklearn
    OUTPUT_FPATH = 'eval_result/'+model+ "_ID_NUM_" + str(MAX_PERSON)+ "_" + dataset +".txt"
