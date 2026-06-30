import unittest

from app.reference_acoustic_signature import (
    cluster_risk_flag_for_label,
    dumb_cluster_display_labels,
    hdbscan_cluster_labels,
    hdbscan_min_cluster_size_for_count,
    normalize_embedding_vector,
)


class ReferenceHdbscanClusteringTests(unittest.TestCase):
    def test_hdbscan_finds_multiple_clusters_on_separated_vectors(self) -> None:
        group_a = [normalize_embedding_vector([1.0, 0.0, 0.0] + [0.0] * 22) for _ in range(12)]
        group_b = [normalize_embedding_vector([0.0, 1.0, 0.0] + [0.0] * 22) for _ in range(12)]
        labels = hdbscan_cluster_labels(group_a + group_b, min_cluster_size=10)
        self.assertEqual(len(labels), 24)
        self.assertGreater(len(set(labels)), 1)

    def test_hdbscan_preserves_noise_bucket(self) -> None:
        dense = [normalize_embedding_vector([1.0, 0.1, 0.0] + [0.0] * 22) for _ in range(12)]
        outlier = [normalize_embedding_vector([0.0, 0.0, 1.0] + [0.0] * 22)]
        labels = hdbscan_cluster_labels(dense + outlier, min_cluster_size=10)
        self.assertIn(-1, labels)

    def test_dumb_cluster_display_labels_number_dense_clusters_and_outliers(self) -> None:
        labels = dumb_cluster_display_labels([17, 17, 23, 23, -1, -1])
        self.assertEqual(labels[17], "1")
        self.assertEqual(labels[23], "2")
        self.assertEqual(labels[-1], "outliers")

    def test_cluster_risk_flag_for_label(self) -> None:
        self.assertEqual(cluster_risk_flag_for_label("1"), "cluster_1")
        self.assertEqual(cluster_risk_flag_for_label("outliers"), "cluster_outliers")

    def test_hdbscan_min_cluster_size_for_count(self) -> None:
        self.assertEqual(hdbscan_min_cluster_size_for_count(141), 5)
        self.assertEqual(hdbscan_min_cluster_size_for_count(451), 13)
        self.assertEqual(hdbscan_min_cluster_size_for_count(4), 5)

    def test_hdbscan_clamps_min_cluster_size_to_sample_count(self) -> None:
        vectors = [normalize_embedding_vector([1.0, 0.0, 0.0] + [0.0] * 22) for _ in range(4)]
        labels = hdbscan_cluster_labels(vectors, min_cluster_size=10)
        self.assertEqual(len(labels), 4)


if __name__ == "__main__":
    unittest.main()
