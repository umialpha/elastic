apiVersion: v1
kind: Pod
metadata:
  name: TBD
  namespace: TBD
  labels:
    job-name: TBD
    worker: TBD
spec:
  restartPolicy: "OnFailure"
  containers:
  - name: elastic-trainer-worker
    image: kuberegistry0.azurecr.io/elastic:v2
    command: ["fetch_and_run"]
    args:
    - "/workspace/elastic/examples/imagenet/main.py"
    - "--input_path"
    - "/data/tiny-imagenet-200/train"
    - "--epochs"
    - "2"
    env:
    - name: RDZV_ENDPOINT
      value: "etcd:2379"
    - name: JOB_ID
      value: "imagenet"
    - name: MIN_SIZE
      value: TBD
    - name: MAX_SIZE
      value: TBD
    resources:
      limits:
        nvidia.com/gpu: 1
    volumeMounts:
    - name: persistent-storage
      mountPath: /data
    - name: dshm
      mountPath: /dev/shm
  volumes:
  - name: persistent-storage
    persistentVolumeClaim:
      claimName: elastic-example
  - name: dshm
    emptyDir:
      medium: Memory

---
apiVersion: v1
kind: Service
metadata:
  name: TBD
  namespace: TBD
  labels:
    job-name: TBD
spec:
  selector:
    job-name: TBD
    worker: TBD
  clusterIP: None
