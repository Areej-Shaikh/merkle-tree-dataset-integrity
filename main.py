import json
import pandas as pd
import hashlib
import time
import tracemalloc
import unicodedata
import re
import uuid

#sha256 hash function
def sha256_hash(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()
#viewing dataset
def view_dataset(path):
    print("Loading 1,500,000 records from dataset...")
    preview_rows = []
    preview_limit = 100
    load_limit = 1_500_000
    with open(path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            data = json.loads(line)
            if idx < preview_limit:
                real_id =(
               data.get("reviewID") or
               data.get("reviewerID") or
               data.get("id") or
               str(uuid.uuid4())
)
                full_text = data.get("reviewText", "")
                short_text = (full_text[:55] + "...") if len(full_text) > 55 else full_text
                preview_rows.append({
                    "index": idx,
                    "review_id": real_id,
                    "asin": data.get("asin", ""),
                    "rating": data.get("overall", ""),
                    "text": short_text
                })
            if idx == load_limit:
                break
    df = pd.DataFrame(preview_rows)
    pd.set_option("display.max_colwidth", 60)
    pd.set_option("display.width", 200)
    print("\n=== FIRST 100 REVIEWS ===\n")
    print(df.to_string(index=False))
#normalizing and cleaning text
def clean_text(text):
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"<.*?>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()

def build_leaf_hashes(path, limit=1_500_000):
    print(f"\nLoading {limit:,} records and generating leaf hashes...")
    leaf_hashes = []
    with open(path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
#limit for 1,500,000 records
            if idx == limit:
                break
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            review_id = (
            data.get("reviewID") or
            data.get("reviewerID") or
            data.get("id") or
            str(uuid.uuid4())
)

            asin = (data.get("asin", "") or "").strip().upper()
            rating = str(data.get("overall", "")).strip()
#clean the review text
            text = clean_text(data.get("reviewText", ""))

#combine hashing onn review_id, asin, rating, text
            combined = review_id + "|" + asin + "|" + rating + "|" + text
            leaf_hash = sha256_hash(combined)
#add to leaf_hashes list
            leaf_hashes.append(leaf_hash)
            if idx % 200_000 == 0:
                print(f"Processed {idx:,} reviews...")
    print(f"\nLeaf Hashes Created: {len(leaf_hashes):,}")
    return leaf_hashes

def build_parent_layer(hashes):
    parent_layer = []
    n = len(hashes)
    for i in range(0, n, 2):
#even
        if i + 1 < n:
            parent_layer.append(sha256_hash(hashes[i] + hashes[i+1]))
        else:
#odd 
            parent_layer.append(sha256_hash(hashes[i]))
    return parent_layer
#merkle root building
def build_merkle_root(leaf_hashes):
    print("\nBuilding Merkle Tree...")
    current_layer = leaf_hashes
    layer = 0
    print(f"Layer {layer}: {len(current_layer):,} nodes")
    while len(current_layer) > 1:
        current_layer = build_parent_layer(current_layer)
        layer += 1
        print(f"Layer {layer}: {len(current_layer):,} nodes")
    print("FINAL MERKLE ROOT:")
    print(current_layer[0])
    return current_layer[0]

def update_leaf_hashes_partial(original_path, tampered_path, original_leaf_hashes):
#update tampered leaf hash 
    updated = original_leaf_hashes.copy()
    with open(original_path, "r", encoding="utf-8") as fo, \
         open(tampered_path, "r", encoding="utf-8") as ft:
        for idx, (lo, lt) in enumerate(zip(fo, ft)):
            if lo != lt:
                data = json.loads(lt)
                review_id = (
                data.get("reviewID") or
                data.get("reviewerID") or
                data.get("id") or
                str(uuid.uuid4())
)

                asin = (data.get("asin", "") or "").strip().upper()
                rating = str(data.get("overall", "")).strip()
                text = clean_text(data.get("reviewText", ""))
                combined = review_id + "|" + asin + "|" + rating + "|" + text
#update the tampered index
                updated[idx] = sha256_hash(combined)
                return updated, idx
    return updated, None


#update the parents of the tampered leaf
def recompute_partial_root(leaf_hashes, changed_index):
    layer = leaf_hashes.copy()
    idx = changed_index
    while len(layer) > 1:
        n = len(layer)
        parent_layer = []
        for i in range(0, n, 2):
            if i + 1 < n:
                parent_layer.append(sha256_hash(layer[i] + layer[i+1]))
            else:
                parent_layer.append(sha256_hash(layer[i]))
        idx //= 2
        layer = parent_layer
    return layer[0]


#checking integrity of dataset
def check_integrity_partial(original_path, tampered_path, saved_root, original_leaf_hashes):
    print("\nChecking dataset integrity...")
#update leaf hashes with tampered dataset
    updated_leaf_hashes, changed_index = update_leaf_hashes_partial(
        original_path, tampered_path, original_leaf_hashes
    )
    if changed_index is None:
        print("No detectable change.")
        return
    print(f"\nFirst detected difference at index: {changed_index}")
#recompute the root after tampering
    new_root = recompute_partial_root(updated_leaf_hashes, changed_index)
    print("\nOriginal Root:", saved_root)
    print("New Root:     ", new_root)
    if new_root == saved_root:
        print("\nDATASET IS INTACT")
    else:
        print("\nTAMPERING DETECTED")


#generating and verifying proof
def generate_proof(index, leaf_hashes):
    proof = []
    current_index = index
    layer = leaf_hashes.copy()

    while len(layer) > 1:
        n = len(layer)

        if current_index % 2 == 0:   # left child
            sibling_index = current_index + 1
            if sibling_index < n:
                proof.append((layer[sibling_index], "right"))
            else:
                proof.append((None, "single"))
        else:                       # right child
            sibling_index = current_index - 1
            proof.append((layer[sibling_index], "left"))

        parent_layer = []
        for i in range(0, n, 2):
            if i + 1 < n:
                parent_layer.append(sha256_hash(layer[i] + layer[i+1]))
            else:
                parent_layer.append(sha256_hash(layer[i]))

        current_index //= 2
        layer = parent_layer

    return proof

def verify_proof(target_hash, proof, merkle_root):
    start_time = time.time()

    computed = target_hash
    for sibling_hash, direction in proof:
        if direction == "left":
            computed = sha256_hash(sibling_hash + computed)
        elif direction == "right":
            computed = sha256_hash(computed + sibling_hash)
        else:
            computed = sha256_hash(computed)

    end_time = time.time()
    elapsed_ms = (end_time - start_time) * 1000

    print(f"\n[VERIFICATION TIME] Proof verified in {elapsed_ms:.2f} ms")

    return computed == merkle_root


def save_root(root, filename="saved_root.txt"):
    with open(filename, "w") as f:
        f.write(root)
    print(f"\nMerkle Root saved to {filename}")

def load_root(filename="saved_root.txt"):
    try:
        with open(filename, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        print("\nNo saved root found.")
        return None

def tamper_menu():
    print("\nTamper Options:")
    print("1. Modify Review #10")
    print("2. Delete Review #10")
    print("3. Insert Fake Review")
    print("0. Cancel")
    return input("Choose tamper type: ")

def tamper_modify(lines):
#modify review text 10
    record = json.loads(lines[10])
    record["reviewText"] = "THIS REVIEW HAS BEEN MODIFIED!"
    lines[10] = json.dumps(record) + "\n"
    print(" Modified record #10")

def tamper_delete(lines):
    lines.pop(10)
    print(" Deleted record #10")
#insert at index 5
def tamper_insert(lines):
    fake = {
        "asin": "fake",
        "overall": 1,
        "reviewText": "THIS IS A FAKE REVIEW!"
    }
    lines.insert(5, json.dumps(fake) + "\n")
    print("Inserted fake review at index #5")

def tamper_dataset(original_path, tampered_path="tampered.json"):
    print("\nSimulating tampering...")
    with open(original_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    option = tamper_menu()
    if option == "1":
        tamper_modify(lines)
    elif option == "2":
        tamper_delete(lines)
    elif option == "3":
        tamper_insert(lines)
    else:
        print("Cancelled.")
        return None
    with open(tampered_path, "w", encoding="utf-8") as f:
        f.writelines(lines)
    print("Tampered dataset saved as tampered.json")
    return tampered_path

def measure_hashing_speed(path, limit=300_000):
    print(f"\nMeasuring hashing speed on first {limit:,} records...")
    start_time = time.time()
    count = 0
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i == limit:
                break
            data = json.loads(line)
            text = (data.get("reviewText", ""))
            sha256_hash(text)
            count += 1
    duration = time.time() - start_time
    speed = count / duration
    print(f"Hashed {count:,} reviews in {duration:.2f}s")
    print(f"Hashing Speed: {speed:,.0f} hashes/sec\n")
    return speed, duration

def measure_merkle_build_performance(leaf_hashes):
    print("\nMeasuring Merkle Tree build performance...")
    tracemalloc.start()
    start_time = time.time()
    root = build_merkle_root(leaf_hashes)
    duration = time.time() - start_time
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_mb = peak / (1024 * 1024)
    print(f"\nMerkle Build Time: {duration:.2f} sec")
    print(f"Peak Memory Usage: {peak_mb:.2f} MB")
    return root, duration, peak_mb

def measure_proof_generation(leaf_hashes, index):
    print("\nMeasuring proof generation time at index 500...")
    start = time.time()
    proof = generate_proof(index, leaf_hashes)
    duration = (time.time() - start) * 1000
    print(f"Proof Generation Time: {duration:.2f} ms")
    return proof, duration

def performance_report(hash_speed, hash_time, build_time, peak_mem, proof_time):
    print("        PERFORMANCE SUMMARY REPORT")
    print(f"Hashing Speed:         {hash_speed:,.0f} hashes/sec")
    print(f"Hashing Time:          {hash_time:.2f} sec")
    print(f"Merkle Build Time:     {build_time:.2f} sec")
    print(f"Peak Memory Usage:     {peak_mem:.2f} MB")
    print(f"Proof Generation Time: {proof_time:.2f} ms")
    print("============================================\n")

    
def run_test_suite():
    print("              RUNNING TEST SUITE")
    tests_passed = 0
    tests_failed = 0

    def run_test(name, description, condition):
        nonlocal tests_passed, tests_failed
        print(f"\nTEST: {name}")
        print(f"Description: {description}")

        if condition:
            print(f"[PASS] {name}")
            tests_passed += 1
        else:
            print(f"[FAIL] {name}")
            tests_failed += 1


    run_test(
        "SHA-256 Hash Function",
        "Checking that sha256_hash of abc matches Python's hashlib output.",
        sha256_hash("abc") == hashlib.sha256("abc".encode()).hexdigest()
    )
#dummy reviews for testing
    dummy_reviews = ["hello", "world"]
    dummy_leafs = [sha256_hash(f"R{i}|||{txt}") for i, txt in enumerate(dummy_reviews)]

    run_test(
        "Leaf Hash Generation",
        "Verifying that 2 dummy reviews (hello and world) produce exactly 2 leaf hashes.",
        len(dummy_leafs) == 2
    )

    p = build_parent_layer(dummy_leafs.copy())
    run_test(
        "Parent Layer Build",
        "Constructing parent layer from 2 leaves should give exactly 1 parent node.",
        len(p) == 1
    )

    root = build_merkle_root(dummy_leafs.copy())
    run_test(
    "Merkle Root Hash Check",
    "Checking if the Merkle root of dummy reviews is a valid 64-character SHA-256 hash.",
    isinstance(root, str) and len(root) == 64
)


    proof = generate_proof(0, dummy_leafs.copy())
    run_test(
        "Proof Generation at index 0",
        "Ensuring a valid proof path is generated for leaf index 0.",
        len(proof) > 0
    )

    valid = verify_proof(dummy_leafs[0], proof, root)
    run_test(
        "Proof Verification",
        "Verifying that the proof reconstructs the Merkle root correctly.",
        valid
    )

    altered = dummy_leafs.copy()
    altered[0] = sha256_hash("tampered")
    run_test(
        "Tampering Detection",
        "Ensuring that tampering changes the leaf hash.",
        dummy_leafs[0] != altered[0]
    )

   
    print("                TEST SUMMARY")
   
    print(f"Tests Passed: {tests_passed}")
    print(f"Tests Failed: {tests_failed}")
 

def menu():
    PATH = "Movies_and_TV_5.json"
    leaf_hashes = None
    merkle_root = None

    while True:
        print("\n====================================")
        print("        MERKLE TREE PROJECT")
        print("====================================")
        print("1. View Dataset")
        print("2. Build Merkle Tree (1.5M)")
        print("3. Save Merkle Root")
        print("4. Load Saved Root")
        print("5. Tamper Dataset")
        print("6. Check Dataset Integrity")
        print("7. Generate + Verify Proof")
        print("8. Run Performance Analysis")
        print("9. Run Test Suite")
        print("0. Exit")
        print("====================================")

        choice = input("Enter choice: ")

        if choice == "1":
            view_dataset(PATH)

        elif choice == "2":
            leaf_hashes = build_leaf_hashes(PATH)
            merkle_root = build_merkle_root(leaf_hashes)

        elif choice == "3":
            if merkle_root:
                save_root(merkle_root)
            else:
                print("Build the tree first.")

        elif choice == "4":
            saved = load_root()
            if saved:
                print("Loaded Root:", saved)

        elif choice == "5":
            tamper_dataset(PATH)

        elif choice == "6":
            saved_root = load_root()
            if not saved_root:
                print("No saved root found!")
                continue
            check_integrity_partial(PATH, "tampered.json", saved_root, leaf_hashes)

        elif choice == "7":
            if leaf_hashes is None or merkle_root is None:
                print("Build the Merkle tree first.")
                continue

            idx = int(input("Enter review index: "))
            target = leaf_hashes[idx]
            proof = generate_proof(idx, leaf_hashes)

            print("\n=== MERKLE PROOF PATH (Sibling Hashes) ===")
            for i, (ph, direction) in enumerate(proof):
                print(f"Step {i+1}: {ph} ({direction})")

            valid = verify_proof(target, proof, merkle_root)
            print("\nVerification Result:", "VALID" if valid else "INVALID")

        elif choice == "8":
            print("\nRunning full performance analysis...")
            hash_speed, hash_time = measure_hashing_speed(PATH)
            leaf_hashes_perf = build_leaf_hashes(PATH)
            root_perf, build_time, peak_mem = measure_merkle_build_performance(leaf_hashes_perf)
            proof, proof_time = measure_proof_generation(leaf_hashes_perf, index=500)
            performance_report(hash_speed, hash_time, build_time, peak_mem, proof_time)

        elif choice == "9":
            run_test_suite()

        elif choice == "0":
            print("Goodbye!")
            break

        else:
            print("Invalid choice.")

if __name__ == "__main__":
    menu()
