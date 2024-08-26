#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <unordered_set>
#include <limits>
#include <cstdlib>
#include <ctime>

// Structure to hold store pairs and their difference
struct store_pair {
    int test_store;
    int control_store;
    double abs_perc_diff;
};

// Function to read the CSV file and store the data in a vector
std::vector<store_pair> read_csv(const std::string& filename) {
    std::ifstream file(filename.c_str());
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
        sp.test_store = std::atoi(word.c_str());

        // Parse the control_store (int)
        std::getline(ss, word, ',');
        sp.control_store = std::atoi(word.c_str());

        // Parse the abs_perc_diff (float)
        std::getline(ss, word, ',');
        sp.abs_perc_diff = std::atof(word.c_str());

        store_pairs.push_back(sp);
    }
    return store_pairs;
}

// Function to calculate the average absolute percentage difference
double calculate_average_difference(const std::vector<store_pair>& matched_pairs) {
    double total_difference = 0.0;
    for (std::vector<store_pair>::const_iterator it = matched_pairs.begin(); it != matched_pairs.end(); ++it) {
        total_difference += it->abs_perc_diff;
    }
    return total_difference / matched_pairs.size();
}

// Traditional comparison function to sort by abs_perc_diff
bool compare_store_pairs(const store_pair& a, const store_pair& b) {
    return a.abs_perc_diff < b.abs_perc_diff;
}

// Generate an initial population of solutions
std::vector<std::vector<store_pair> > generate_initial_population(const std::vector<store_pair>& store_pairs, int population_size, int solution_size) {
    std::vector<std::vector<store_pair> > population;
    std::srand(static_cast<unsigned>(std::time(0)));

    for (int i = 0; i < population_size; ++i) {
        std::vector<store_pair> solution;
        std::unordered_set<int> used_test_stores;
        std::unordered_set<int> used_control_stores;
        std::vector<int> indices(store_pairs.size());
        for (size_t j = 0; j < indices.size(); ++j) indices[j] = j;
        std::random_shuffle(indices.begin(), indices.end());

        for (size_t j = 0; j < store_pairs.size(); ++j) {
            const store_pair& sp = store_pairs[indices[j]];
            if (used_test_stores.count(sp.test_store) == 0 && used_control_stores.count(sp.control_store) == 0) {
                solution.push_back(sp);
                used_test_stores.insert(sp.test_store);
                used_control_stores.insert(sp.control_store);
                if (solution.size() == solution_size) break;
            }
        }
        population.push_back(solution);
    }

    return population;
}

// Perform crossover between two parent solutions
std::vector<store_pair> crossover(const std::vector<store_pair>& parent1, const std::vector<store_pair>& parent2) {
    std::vector<store_pair> child;
    std::unordered_set<int> used_test_stores;
    std::unordered_set<int> used_control_stores;

    // First half from parent1
    for (size_t i = 0; i < parent1.size() / 2; ++i) {
        child.push_back(parent1[i]);
        used_test_stores.insert(parent1[i].test_store);
        used_control_stores.insert(parent1[i].control_store);
    }

    // Second half from parent2, avoiding duplicates
    for (size_t i = 0; i < parent2.size(); ++i) {
        if (used_test_stores.count(parent2[i].test_store) == 0 && used_control_stores.count(parent2[i].control_store) == 0) {
            child.push_back(parent2[i]);
            used_test_stores.insert(parent2[i].test_store);
            used_control_stores.insert(parent2[i].control_store);
        }
        if (child.size() == parent1.size()) break;
    }

    return child;
}

// Perform mutation on a solution
void mutate(std::vector<store_pair>& solution, const std::vector<store_pair>& store_pairs, double mutation_rate) {
    if (static_cast<double>(std::rand()) / RAND_MAX < mutation_rate) {
        size_t index_to_replace = std::rand() % store_pairs.size();
        const store_pair& new_pair = store_pairs[std::rand() % store_pairs.size()];
        std::unordered_set<int> used_test_stores;
        std::unordered_set<int> used_control_stores;

        for (size_t i = 0; i < solution.size(); ++i) {
            used_test_stores.insert(solution[i].test_store);
            used_control_stores.insert(solution[i].control_store);
        }

        if (used_test_stores.count(new_pair.test_store) == 0 && used_control_stores.count(new_pair.control_store) == 0) {
            solution[index_to_replace] = new_pair;
        }
    }
}

// Evolve the population using genetic algorithms with early stopping
std::vector<store_pair> genetic_algorithm(const std::vector<store_pair>& store_pairs, int population_size, int generations, double mutation_rate, double target_difference) {
    int solution_size = 100;
    std::vector<std::vector<store_pair> > population = generate_initial_population(store_pairs, population_size, solution_size);
    std::vector<store_pair> best_solution;
    double best_fitness = std::numeric_limits<double>::max();

    for (int generation = 0; generation < generations; ++generation) {
        std::vector<std::pair<double, std::vector<store_pair> > > fitness_population;

        // Evaluate fitness (average difference) of each solution
        for (size_t i = 0; i < population.size(); ++i) {
            double fitness = calculate_average_difference(population[i]);
            fitness_population.push_back(std::make_pair(fitness, population[i]));
        }

        // Sort population by fitness (lower is better)
        std::sort(fitness_population.begin(), fitness_population.end(), compare_store_pairs);

        // Check for early stopping
        if (fitness_population[0].first <= target_difference) {
            std::cout << "Early stopping at generation " << generation << " with average difference: " << fitness_population[0].first << std::endl;
            return fitness_population[0].second;
        }

        // Keep track of the best solution so far
        if (fitness_population[0].first < best_fitness) {
            best_fitness = fitness_population[0].first;
            best_solution = fitness_population[0].second;
        }

        // Create a new population with the best solutions
        std::vector<std::vector<store_pair> > new_population;

        for (int i = 0; i < population_size / 2; ++i) {
            // Select parents
            const std::vector<store_pair>& parent1 = fitness_population[i].second;
            const std::vector<store_pair>& parent2 = fitness_population[i + 1].second;

            // Perform crossover
            std::vector<store_pair> child1 = crossover(parent1, parent2);
            std::vector<store_pair> child2 = crossover(parent2, parent1);

            // Perform mutation
            mutate(child1, store_pairs, mutation_rate);
            mutate(child2, store_pairs, mutation_rate);

            new_population.push_back(child1);
            new_population.push_back(child2);
        }

        population = new_population;

        // Optionally print progress
        if (generation % 10 == 0) {
            std::cout << "Generation " << generation << ", Best Average Difference: " << fitness_population[0].first << std::endl;
        }
    }

    // Return the best solution found if early stopping did not occur
    return best_solution;
}

// Greedy solution to calculate the initial baseline average difference
std::vector<store_pair> greedy_solution(const std::vector<store_pair>& store_pairs) {
    std::vector<store_pair> solution;
    std::unordered_set<int> used_test_stores;
    std::unordered_set<int> used_control_stores;

    // Sort the pairs by absolute percentage difference
    std::vector<store_pair> sorted_pairs = store_pairs;
    std::sort(sorted_pairs.begin(), sorted_pairs.end(), compare_store_pairs);

    // Select the best 100 pairs greedily
    for (size_t i = 0; i < sorted_pairs.size(); ++i) {
        const store_pair& sp = sorted_pairs[i];
        if (used_test_stores.count(sp.test_store) == 0 && used_control_stores.count(sp.control_store) == 0) {
            solution.push_back(sp);
            used_test_stores.insert(sp.test_store);
            used_control_stores.insert(sp.control_store);
            if (solution.size() == 100) break;
        }
    }

    return solution;
}

// Function to write matched pairs to a CSV file
void write_csv(const std::string& filename, const std::vector<store_pair>& matched_pairs) {
    std::ofstream file(filename.c_str());
    file << "test_store,control_store,abs_perc_diff\n";
    for (size_t i = 0; i < matched_pairs.size(); ++i) {
        file << matched_pairs[i].test_store << ","
             << matched_pairs[i].control_store << ","
             << matched_pairs[i].abs_perc_diff << "\n";
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <csv_file_path>" << std::endl;
        return 1;
    }

    std::string filename = argv[1];  // File path provided as a command-line argument
    std::vector<store_pair> store_pairs = read_csv(filename);

    // Compute the greedy solution
    std::vector<store_pair> greedy_solution_result = greedy_solution(store_pairs);
    double greedy_avg_diff = calculate_average_difference(greedy_solution_result);
    std::cout << "Greedy Solution Average Difference: " << greedy_avg_diff << std::endl;

    // Set target improvement (e.g., 10% improvement over greedy solution)
    double target_improvement = 0.10;  // 10% improvement
    double target_difference = greedy_avg_diff * (1.0 - target_improvement);

    // Apply genetic algorithm optimization with early stopping
    int population_size = 100;
    int generations = 1000;
    double mutation_rate = 0.05;

    std::vector<store_pair> best_solution = genetic_algorithm(store_pairs, population_size, generations, mutation_rate, target_difference);
    double best_avg_diff = calculate_average_difference(best_solution);
    write_csv("genetic_algorithm_best_solution.csv", best_solution);

    std::cout << "Best Solution Average Difference: " << best_avg_diff << std::endl;

    return 0;
}
