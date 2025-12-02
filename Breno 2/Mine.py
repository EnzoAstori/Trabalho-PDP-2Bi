
import paho.mqtt.client as mqtt
import argparse
import json
import random
import threading
import time
import ssl
import hashlib
import sys


DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "Lasanha1"

TOP_BASE = "sd/"
TOP_INIT = TOP_BASE + "init"
TOP_VOTE = TOP_BASE + "vote"
TOP_LEADER = TOP_BASE + "leader"      
TOP_CHALLENGE = TOP_BASE + "challenge"
TOP_SOLUTION = TOP_BASE + "solution"
TOP_RESULT = TOP_BASE + "result"

QOS = 1


class Node:
    def __init__(self, broker, port, participants, username, password, tls_insecure=True):
        self.broker = broker
        self.port = port
        self.participants = participants
        self.username = username
        self.password = password
        self.tls_insecure = tls_insecure

        
        self.client_id = random.randint(10000, 99999)
        self.my_vote = random.randint(10000, 99999)

    
        self.inits = set()        
        self.votes = {}           
        self.leader = None        
        self.is_leader = False

        
        self.next_tx = 0
        
        self.challenges = {}
        
        self.closed_txs = set()  

        self.inits_event = threading.Event()
        self.votes_event = threading.Event()

      
        self.client = mqtt.Client(client_id=str(self.client_id), clean_session=True)
        self.client.username_pw_set(self.username, self.password)

        
        self.client.tls_set(cert_reqs=ssl.CERT_NONE)
        if tls_insecure:
            self.client.tls_insecure_set(True)

        
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

        
        self.running = True

        
        print(f"[Client {self.client_id}] Inicializando (vote={self.my_vote}) -> conectando {broker}:{port}")
        self.client.connect(self.broker, self.port, keepalive=60)
        self.client.loop_start()

        
        self._subscribe_topics()

        
        threading.Thread(target=self._publish_init_window, daemon=True).start()

    

    def _subscribe_topics(self):
        
        self.client.subscribe(TOP_INIT, qos=QOS)
        self.client.subscribe(TOP_VOTE, qos=QOS)
        self.client.subscribe(TOP_LEADER, qos=QOS)
        self.client.subscribe(TOP_CHALLENGE, qos=QOS)
        self.client.subscribe(TOP_SOLUTION, qos=QOS)
        self.client.subscribe(TOP_RESULT, qos=QOS)
        print(f"[Client {self.client_id}] Inscrito nos tópicos.")

    def _on_connect(self, client, userdata, flags, rc):
        print(f"[Client {self.client_id}] on_connect rc={rc}")

    def _on_message(self, client, userdata, msg):
      
        if not msg.payload or msg.payload.decode().strip() == "":
            return
        try:
            data = json.loads(msg.payload.decode())
        except Exception:
           
            return

        topic = msg.topic
        if topic == TOP_INIT:
            self._handle_init(data)
        elif topic == TOP_VOTE:
            self._handle_vote(data)
        elif topic == TOP_LEADER:
            self._handle_leader_announce(data)
        elif topic == TOP_CHALLENGE:
            self._handle_challenge(data)
        elif topic == TOP_SOLUTION:
            self._handle_solution(data)
        elif topic == TOP_RESULT:
            self._handle_result(data)

 
    def _publish_init_window(self):
       
        time.sleep(0.2)
        msg = {"id": self.client_id}
        try:
          
            self.client.publish(TOP_INIT, json.dumps(msg), qos=QOS, retain=True)
        except Exception as e:
            print(f"[Client {self.client_id}] erro ao publicar INIT retained: {e}")

        start = time.time()
        window = 3.0  
        while time.time() - start < window:
            try:
                self.client.publish(TOP_INIT, json.dumps(msg), qos=QOS, retain=False)
            except:
                pass
            time.sleep(0.4)

    def _handle_init(self, payload):
        nid = payload.get("id")
        if nid is None:
            return
        if nid not in self.inits:
            self.inits.add(nid)
            print(f"[Client {self.client_id}] INIT recebida de {nid}. Total: {len(self.inits)}/{self.participants}")
        
        if len(self.inits) >= self.participants:
            self.inits_event.set()


    def wait_and_publish_vote(self, timeout=8):
     
        got = self.inits_event.wait(timeout=timeout)
        if not got:
            print(f"[Client {self.client_id}] timeout aguardando INITs ({len(self.inits)}/{self.participants}), seguirei com o que tenho.")
        
        self.votes[self.client_id] = self.my_vote
        payload = {"id": self.client_id, "vote": self.my_vote}
        self.client.publish(TOP_VOTE, json.dumps(payload), qos=QOS, retain=False)
        print(f"[Client {self.client_id}] Vote publicado: {self.my_vote}")
       
        time.sleep(0.2)
        self.client.publish(TOP_VOTE, json.dumps(payload), qos=QOS, retain=False)

    def _handle_vote(self, payload):
        nid = payload.get("id")
        vote = payload.get("vote")
        if nid is None or vote is None:
            return
        
        try:
            vote_int = int(vote)
        except:
            return
        if nid not in self.votes:
            self.votes[nid] = vote_int
            print(f"[Client {self.client_id}] Voto recebido de {nid}: {vote_int}. Total {len(self.votes)}/{self.participants}")
        if len(self.votes) >= self.participants:
            self.votes_event.set()

    def determine_leader(self, timeout=8):
        got = self.votes_event.wait(timeout=timeout)
        if not got:
            print(f"[Client {self.client_id}] timeout aguardando votos ({len(self.votes)}/{self.participants}). Decidindo com votos recebidos.")
        
        valid = {k: v for k, v in self.votes.items() if isinstance(v, int)}
        if not valid:
            print(f"[Client {self.client_id}] nenhum voto válido disponível.")
            return False
        winner = max(valid.items(), key=lambda kv: (kv[1], kv[0]))[0]
        self.leader = winner
        self.is_leader = (self.leader == self.client_id)
        print(f"[Client {self.client_id}] LÍDER eleito: {self.leader} (eu sou líder? {self.is_leader})")
        
        ann = {"leader": self.leader}
        self.client.publish(TOP_LEADER, json.dumps(ann), qos=QOS, retain=True)
        return True

    def _handle_leader_announce(self, payload):
        leader = payload.get("leader")
        if leader is None:
            return
        prev = self.leader
        self.leader = leader
        self.is_leader = (self.leader == self.client_id)
        if prev != leader:
            print(f"[Client {self.client_id}] Anúncio de líder: {self.leader} (eu sou líder? {self.is_leader})")
            
            if self.is_leader:
                threading.Thread(target=self._leader_loop, daemon=True).start()

 
    def _leader_loop(self):
        
        while self.running and self.is_leader:
            tx = self.next_tx
            self.next_tx += 1
            difficulty = random.randint(2, 5)   
            
            self.challenges[(self.client_id, tx)] = difficulty
            payload = {"leader": self.client_id, "tx": tx, "difficulty": difficulty}
            self.client.publish(TOP_CHALLENGE, json.dumps(payload), qos=QOS, retain=False)
            print(f"[LÍDER {self.client_id}] Novo desafio TX={tx} difficulty={difficulty}")
           
            time.sleep(4)

    def _handle_challenge(self, payload):
        leader = payload.get("leader")
        tx = payload.get("tx")
        difficulty = payload.get("difficulty")
        if leader is None or tx is None or difficulty is None:
            return

        self.challenges[(leader, tx)] = difficulty
        print(f"[Client {self.client_id}] Desafio recebido TX={tx} lider={leader} dif={difficulty} -> minerando...")
        threading.Thread(target=self._mine_and_submit, args=(leader, tx, difficulty), daemon=True).start()

    
    def _mine_and_submit(self, leader, tx, difficulty):
        target = "0" * difficulty
       
        attempts = 0
        while self.running:
            candidate = ''.join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=20))
            
            sha = hashlib.sha1(candidate.encode()).hexdigest()
            attempts += 1
            if sha.startswith(target):
                sol = {
                    "id": self.client_id,
                    "leader": leader,
                    "tx": tx,
                    "solution": candidate
                }
                self.client.publish(TOP_SOLUTION, json.dumps(sol), qos=QOS, retain=False)
                print(f"[Minerador {self.client_id}] SOLUÇÃO encontrada TX={tx} para leader={leader}: {candidate}")
                return
        
            if attempts % 1000 == 0:
                time.sleep(0)  

   
    def _handle_solution(self, payload):
        leader = payload.get("leader")
        tx = payload.get("tx")
        miner = payload.get("id")
        solution = payload.get("solution")
        if leader is None or tx is None or miner is None or solution is None:
            return

       
        if not self.is_leader or self.client_id != leader:
          
            return

        key = (leader, tx)
        if key in self.closed_txs:
           
            print(f"[LÍDER {self.client_id}] TX={tx} já fechado — ignorando solução de {miner}")
            return

        difficulty = self.challenges.get(key)
        if difficulty is None:
            print(f"[LÍDER {self.client_id}] Solução TX={tx} recebida, mas desafio desconhecido.")
            return

        sha = hashlib.sha1(solution.encode()).hexdigest()
        if sha.startswith("0" * difficulty):
           
            print(f"[LÍDER {self.client_id}] Solução VÁLIDA de {miner} para TX={tx}")
            res = {"leader": leader, "tx": tx, "winner": miner, "valid": True}
           
            self.closed_txs.add(key)
            self.client.publish(TOP_RESULT, json.dumps(res), qos=QOS, retain=False)
        else:
            print(f"[LÍDER {self.client_id}] Solução INVÁLIDA de {miner} para TX={tx}")

 
    def _handle_result(self, payload):
       
        if "leader" in payload and "tx" not in payload:
            
            leader = payload.get("leader")
            prev = self.leader
            self.leader = leader
            self.is_leader = (self.leader == self.client_id)
            if prev != leader:
                print(f"[Client {self.client_id}] Anúncio de líder (via result): {self.leader} (eu sou líder? {self.is_leader})")
                if self.is_leader:
                    threading.Thread(target=self._leader_loop, daemon=True).start()
            return

       
        tx = payload.get("tx")
        winner = payload.get("winner")
        leader = payload.get("leader")
        valid = payload.get("valid", None)
        if tx is None or winner is None or leader is None:
            return
        print(f"[Client {self.client_id}] RESULT TX={tx} (leader={leader}) -> winner={winner} valid={valid}")
      
        self.closed_txs.add((leader, tx))

  
    def run(self):
        try:
           
            time.sleep(0.6)
            print(f"[Client {self.client_id}] Aguardando INITs (esperando {self.participants})...")
            
            self.wait_and_publish_vote(timeout=6)
            
            time.sleep(0.3)
            self.client.publish(TOP_VOTE, json.dumps({"id": self.client_id, "vote": self.my_vote}), qos=QOS, retain=False)
            
            self.determine_leader(timeout=8)
           
            if self.is_leader:
               
                threading.Thread(target=self._leader_loop, daemon=True).start()
            
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Encerrando por KeyboardInterrupt...")
        finally:
            self.running = False
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except:
                pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--participants", required=True, type=int)
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--tls_insecure", action="store_true", help="Set TLS insecure (skip cert verification)")
    args = parser.parse_args()

    node = Node(
        broker=args.broker,
        port=args.port,
        participants=args.participants,
        username=args.username,
        password=args.password,
        tls_insecure=args.tls_insecure
    )
    node.run()

if __name__ == "__main__":
    main()
