import uuid
import unittest

from src.pgfn_storage import ObjectStorage, artifact_root, storage_child


class ObjectStorageTest(unittest.TestCase):
    def test_memory_backend_simulates_bucket(self) -> None:
        root = f"memory://pgfn-tests/{uuid.uuid4().hex}"
        storage = ObjectStorage(root)
        location = storage.write_bytes("trf1/report.json", b"conteudo")

        self.assertTrue(storage.exists("trf1/report.json"))
        self.assertEqual(storage.read_bytes("trf1/report.json"), b"conteudo")
        self.assertTrue(str(location).startswith("memory://"))

    def test_local_children_remain_paths(self) -> None:
        root = artifact_root("dag", "/tmp/pgfn-artifacts")
        self.assertEqual(storage_child(root, "pdfs"), root / "pdfs")


if __name__ == "__main__":
    unittest.main()
