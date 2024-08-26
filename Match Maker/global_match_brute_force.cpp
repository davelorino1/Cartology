#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <unordered_set>
#include <cmath>
#include <numeric>
#include <iterator>

// Structure to hold store pairs and their difference
struct store_pair {
    int test_store;
    int control_store;
    double abs_perc_diff;
};

// Function to read the CSV file and store the data in a vector
std::vector<store_pair> read_csv(const std::string& filename) {
    std::ifstream file(filename);
    std::string line, word;
    std::vector<store_pair> store_pairs;

    // Skip the header row
    if (std::getline(file, line)) {
        // Optionally, verify that the header matches the expected format here
    }

    while (std::getline(file, line)) {
        std::stringstream ss(line);
        store_pair sp;

        // Ignore the study_id
        std::getline(ss, word, ','); // Skip the first column

        // Parse the test_store (int)
        std::getline(ss, word, ',');
        sp.test_store = std::stoi(word);

        // Parse the control_store (int)
        std::getline(ss, word, ',');
        sp.control_store = std::stoi(word);

        // Parse the abs_perc_diff (float)
        std::getline(ss, word, ',');
        sp.abs_perc_diff = std::stod(word);

        store_pairs.push_back(sp);
    }
    return store_pairs;
}

// Function to calculate the average absolute percentage difference
double calculate_average_difference(const std::vector<store_pair>& matched_pairs) {
    double total_difference = 0.0;
    for (const auto& pair : matched_pairs) {
        total_difference += pair.abs_perc_diff;
    }
    return total_difference / matched_pairs.size();
}

// Helper function to check if a combination of pairs is valid
bool is_valid_combination(const std::vector<store_pair>& combination) {
    std::unordered_set<int> used_test_stores;
    std::unordered_set<int> used_control_stores;

    for (const auto& sp : combination) {
        if (used_test_stores.count(sp.test_store) > 0 || used_control_stores.count(sp.control_store) > 0) {
            return false; // Repetition found
        }
        used_test_stores.insert(sp.test_store);
        used_control_stores.insert(sp.control_store);
    }
    return true;
}

// Function to find the best combination of 100 pairs by brute force
std::vector<store_pair> global_matching_brute_force(const std::vector<store_pair>& store_pairs) {
    std::vector<store_pair> best_combination;
    double min_avg_difference = std::numeric_limits<double>::max();

    // Generate all possible combinations of 100 pairs
    std::vector<int> indices(store_pairs.size());
    std::iota(indices.begin(), indices.end(), 0);
    std::vector<int> combination(100);

    do {
        // Generate current combination
        for (int i = 0; i < 100; ++i) {
            combination[i] = indices[i];
        }

        // Create the current combination of store pairs
        std::vector<store_pair> current_combination;
        for (int i = 0; i < 100; ++i) {
            current_combination.push_back(store_pairs[combination[i]]);
        }

        // Check if the combination is valid (no repetition)
        if (is_valid_combination(current_combination)) {
            double avg_difference = calculate_average_difference(current_combination);

            if (avg_difference < min_avg_difference) {
                min_avg_difference = avg_difference;
                best_combination = current_combination;
            }
        }
    } while (std::next_permutation(indices.begin(), indices.end()));

    return best_combination;
}

// Function to write matched pairs to a CSV file
void write_csv(const std::string& filename, const std::vector<store_pair>& matched_pairs) {
    std::ofstream file(filename);
    file << "test_store,control_store,abs_perc_diff\n";
    for (const auto& pair : matched_pairs) {
        file << pair.test_store << ","
             << pair.control_store << ","
             << pair.abs_perc_diff << "\n";
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <csv_file_path>" << std::endl;
        return 1;
    }

    std::string filename = argv[1];  // File path provided as a command-line argument
    std::vector<store_pair> store_pairs = read_csv(filename);

    // Apply brute force global optimization and select the best 100 pairs
    std::vector<store_pair> global_result = global_matching_brute_force(store_pairs);
    double global_avg_diff = calculate_average_difference(global_result);
    write_csv("global_matching_brute_force.csv", global_result);

    std::cout << "Global Matching Average Difference (Top 100): " << global_avg_diff << std::endl;

    return 0;
}
