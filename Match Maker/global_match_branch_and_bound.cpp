#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <unordered_set>
#include <limits>
#include <chrono>
#include <iomanip>

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

// Traditional comparison function to replace lambda
bool compare_store_pairs(const store_pair& a, const store_pair& b) {
    return a.abs_perc_diff < b.abs_perc_diff;
}

// Estimate the total number of branches for the recursive search
int estimate_total_branches(int total_pairs, int depth) {
    int branches = 1;
    for (int i = 0; i < depth; ++i) {
        branches *= (total_pairs - i);
    }
    return branches;
}

// Backtracking function to explore combinations with progress reporting
void branch_and_bound(const std::vector<store_pair>& store_pairs, std::vector<store_pair>& current_selection,
                      std::vector<store_pair>& best_selection, std::unordered_set<int>& used_test_stores,
                      std::unordered_set<int>& used_control_stores, double& best_avg_diff, int index,
                      int total_branches, int& branches_explored, int& progress_marker,
                      std::chrono::steady_clock::time_point start_time) {

    // If we have selected 100 pairs, evaluate the current average difference
    if (current_selection.size() == 100) {
        double current_avg_diff = calculate_average_difference(current_selection);
        if (current_avg_diff < best_avg_diff) {
            best_avg_diff = current_avg_diff;
            best_selection = current_selection;
        }
        branches_explored++;
        return;
    }

    // If we reach the end of the store pairs, return
    if (index >= store_pairs.size()) return;

    // For each remaining pair, consider adding it to the current selection
    for (int i = index; i < store_pairs.size(); ++i) {
        const auto& sp = store_pairs[i];

        // Skip if the test or control store has already been used
        if (used_test_stores.count(sp.test_store) > 0 || used_control_stores.count(sp.control_store) > 0) continue;

        // Add this pair to the current selection
        current_selection.push_back(sp);
        used_test_stores.insert(sp.test_store);
        used_control_stores.insert(sp.control_store);

        // Recurse deeper with the updated selection
        branch_and_bound(store_pairs, current_selection, best_selection, used_test_stores, used_control_stores, best_avg_diff, i + 1, total_branches, branches_explored, progress_marker, start_time);

        // Backtrack: remove the pair and try the next one
        current_selection.pop_back();
        used_test_stores.erase(sp.test_store);
        used_control_stores.erase(sp.control_store);

        // Update progress and print only at each 1% completion
        branches_explored++;
        int progress = (branches_explored * 100) / total_branches;
        if (progress > progress_marker && progress <= 100) {
            progress_marker = progress;
            auto current_time = std::chrono::steady_clock::now();
            std::chrono::duration<double> elapsed_seconds = current_time - start_time;

            // Display progress bar and time elapsed
            std::cout << "\rProgress: [";
            int pos = progress / 2;
            for (int j = 0; j < 50; ++j) {
                if (j < pos) std::cout << "=";
                else if (j == pos) std::cout << ">";
                else std::cout << " ";
            }
            std::cout << "] " << progress << "%, Elapsed Time: " << std::setw(6) << std::fixed << std::setprecision(2) << elapsed_seconds.count() << "s";
            std::cout.flush();
        }
    }
}

// Function to perform branch-and-bound global optimization with progress reporting
std::vector<store_pair> global_matching_branch_and_bound(std::vector<store_pair>& store_pairs) {
    std::vector<store_pair> best_selection;
    std::vector<store_pair> current_selection;
    std::unordered_set<int> used_test_stores;
    std::unordered_set<int> used_control_stores;
    double best_avg_diff = std::numeric_limits<double>::max();

    // Sort by abs_perc_diff globally to prioritize smaller differences
    std::sort(store_pairs.begin(), store_pairs.end(), compare_store_pairs);

    // Estimate total number of branches (this is just a rough estimate)
    int total_branches = estimate_total_branches(store_pairs.size(), 100);
    int branches_explored = 0;
    int progress_marker = 0;

    // Start the branch-and-bound process
    auto start_time = std::chrono::steady_clock::now();
    branch_and_bound(store_pairs, current_selection, best_selection, used_test_stores, used_control_stores, best_avg_diff, 0, total_branches, branches_explored, progress_marker, start_time);

    std::cout << std::endl;  // Move to a new line after progress bar completes

    return best_selection;
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

    // Apply branch-and-bound global optimization with progress reporting
    std::vector<store_pair> global_result = global_matching_branch_and_bound(store_pairs);
    double global_avg_diff = calculate_average_difference(global_result);
    write_csv("global_matching_branch_and_bound.csv", global_result);

    std::cout << "Global Matching Average Difference (Top 100): " << global_avg_diff << std::endl;

    return 0;
}
