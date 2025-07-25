#include <iostream>
#include <fstream>
#include "TFile.h"
#include "TTree.h"

const uint16_t CB_MARKER = 0xCBCB;
const uint16_t EVENT_MARKER = 0xEFFE;
const uint16_t TIME_FRAGMENT_MARKER = 0xFEEF;

// Structure definitions based on the binary format

struct Event {
    //uint16_t magic;     // 0xEFFE
    uint64_t event_id;
    uint64_t timestamp;
    //uint32_t n_bytes;
    uint32_t n_cbs;
};

struct CBData {
    uint16_t magic;     // 0xCBCB
    uint8_t cb_id;
    uint16_t n_robs;
    // ROB data would follow here, but not specified in format
};

void readBinaryToRoot(const char* inputFilename, const char* outputFilename) {
    // Open input binary file
    std::ifstream input(inputFilename, std::ios::binary);
    if (!input) {
        std::cerr << "Error opening input file: " << inputFilename << std::endl;
        return;
    }

    /*
    // Create ROOT file and tree
    TFile output(outputFilename, "RECREATE");
    TTree tree("data", "Event data from binary file");

    // Variables to store in the tree
    uint64_t event_id, event_timestamp;
    uint8_t cb_id;
    uint16_t n_robs;
    
    // Branch setup
    tree.Branch("event_id", &event_id, "event_id/l");
    tree.Branch("event_timestamp", &event_timestamp, "event_timestamp/l");
    tree.Branch("cb_id", &cb_id, "cb_id/b");
    tree.Branch("n_robs", &n_robs, "n_robs/s");

    TimeFragment tf;
    Event evt;
    CBData cb;
*/
   Event evt;


    while (input) {
        // Read marker (assuming 2 bytes)
        uint16_t marker;
        input.read(reinterpret_cast<char*>(&marker), sizeof(marker));
        
        // Handle different markers
        if (marker == EVENT_MARKER) {
	  input.read(reinterpret_cast<char*>(&evt), sizeof(Event));
            if (!input) break;
	  std::cout << evt.event_id << std::endl;   
            // Read event data

        }
        else if (marker == TIME_FRAGMENT_MARKER) {
            // Handle time fragment
            // ...
        }
        else if (marker == CB_MARKER) {
            // Handle CB marker
            // ...
        }
    }

    //output.Write();
    //output.Close();
    input.close();
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] << " <input_binary_file> <output_root_file>" << std::endl;
        return 1;
    }

    readBinaryToRoot(argv[1], argv[2]);
    return 0;
}
