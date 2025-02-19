#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <vector>

int main() {
  std::srand(std::time(nullptr));

  std::vector<std::string> logLevels = {"ERROR", "INFO", "WARN"};
  std::vector<int> counts = std::vector<int>(logLevels.size(), 0);

  for (int i = 1; i <= 10; i++) {
    std::string fileName = "log_" + std::to_string(i) + ".txt";
    std::ofstream file(fileName);

    if (!file) {
      std::cerr << "Error creating file: " << fileName << std::endl;
      continue;
    }

    for (int j = 0; j < 100; j++) {
      const int randomIndex = std::rand() % logLevels.size();

      counts[randomIndex]++;
      file << logLevels[randomIndex] << "\n";
    }

    file.close();
    std::cout << "generated log file: " << fileName << std::endl;
  }

  std::cout << "counts:" << std::endl;
  for (int i = 0; i < counts.size(); i++) {
    std::cout << logLevels[i] << ": " << counts[i] << std::endl;
  }

  return 0;
}
