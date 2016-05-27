__author__ = "Jeremy Nelson"
import redis
import redisbayes

def generate_work_tokens(
    work_digest, 
    datastore=redis.StrictRedis()):
    def extend_tokens(result):
        for row in result:
            first_key, second_key = row.decode().split(":")
            value = datastore.get(second_key)
            tokens.extend([word.lower() for word in value.split()])

    tokens = []
    work_pred_objs = "{}:pred-obj".format(work_digest)
    total_triples = datastore.scard(work_pred_objs)
    # Checks for bf:title SHA1
    bf_title_result = datastore.sscan(work_pred_objs,
        match="e366a989e4becead9409ca4d44ddf307afc126b3:*",
        count=total_triples)
    if len(bf_title_result[1]) > 0:
        extend_tokens(bf_title_result[1])
    # Try bf:workTitle SHA1  
    else:
        bf_work_title_result = datastore.sscan(work_pred_objs,
            match="f610f749c5c2eaf6718eb2bc24bf74559d14637d:*",
            count=total_triples)
        if len(bf_work_title_result[1]) > 0:
            for row in bf_work_title_result[1]:
                rdf_title_key = row.decode().split(":")[1]
                # Retrieves the bf:titleValue from Title instance
                rdf_title_value_result = datastore.sscan(
                    "{}:pred-obj".format(rdf_title_key),
                    match="0859add153c1fcda5e32853e22ccfe8514702b2e:*")
                if len(rdf_title_value_result[1]) > 0:
                    extend_tokens(rdf_title_value_result[1])
    # bf:creator and bf:contributor digests
    for key_digest in ["0f08c96e756a4fa720257bf3090efdf76b5d3acc",
                       "a20301af19937f3787275c059dae953eaff2cb5f"]:
        bf_result = datastore.sscan(
            work_pred_objs,
            match="{}:*".format(key_digest),
            count=total_triples)
        if len(bf_result[1]) > 0:
            for row in bf_result[1]:
                agent_key = row.decode().split(":")[1]
                # Scans for bf:label in Person or Organization
                agent_scan_result = datastore.sscan(
                    "{}:pred-obj".format(agent_key),
                    match="56375fdb9714268c237e4eb7e74f6f0544098935:*",
                    count=100)
                if len(agent_scan_result[1]) > 0:
                    extend_tokens(agent_scan_result[1])
    return tokens

        
def train(datastore=redis.StrictRedis()):
    rb = redisbayes.RedisBayes(redis=datastore)

    # Retrieves all Works from the bf-training set
    for work_digest in datastore.smembers('bf-training'):
        tokens = generate_work_tokens(work_digest, datastore)
        print(tokens)
        classify = input("""Classify this BIBFRAME Work as either
        pp - Pride and Prejudice
        md - Moby Dick
        uk - Unknown
        >>""")
        rb.train(classify, ' '.join(tokens))
