import pandas as pd
import os
import re
import glob
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backup/sas_extract.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SASExtractor:
    """Extract lookup data from SAS format file and save as CSV files."""

    def __init__(self, sas_format_path, staging_dir='staging'):
        """
        Initialize the SAS extractor.

        Args:
            sas_format_path (str): Path to the SAS format file
            staging_dir (str): Directory to save the output CSV files
        """
        self.sas_format_path = sas_format_path
        self.staging_dir = staging_dir

        # Create output directory if it doesn't exist
        os.makedirs(staging_dir, exist_ok=True)

        # Dictionary to store the extracted lookup data
        self.lookup_dicts = {}

        logger.info("SAS extractor initialized")

    def parse_sas_format_file(self):
        """Parse SAS format file to create lookup dictionaries."""
        logger.info(f"Parsing SAS format file: {self.sas_format_path}")

        try:
            with open(self.sas_format_path, 'r') as f:
                content = f.read()
        except FileNotFoundError:
            logger.error(f"SAS format file not found at {self.sas_format_path}")
            return False
        except Exception as e:
            logger.error(f"Error reading SAS format file: {e}")
            return False

        # Extract i94cntyl format (country codes)
        i94cntyl_match = re.search(r'value i94cntyl(.*?);', content, re.DOTALL)
        if i94cntyl_match:
            logger.info("Extracting country codes (i94cntyl)")
            i94cntyl_text = i94cntyl_match.group(1)
            i94cntyl_dict = {}
            for line in i94cntyl_text.strip().split('\n'):
                line = line.strip()
                if '=' in line:
                    code, country = line.split('=', 1)
                    code = code.strip()
                    country = country.strip().strip("'").strip()
                    try:
                        i94cntyl_dict[int(code)] = country
                    except ValueError:
                        # Handle non-integer codes
                        i94cntyl_dict[code] = country
            self.lookup_dicts['i94cntyl'] = i94cntyl_dict

        # Extract i94prtl format (port codes)
        i94prtl_match = re.search(r'value \$i94prtl(.*?);', content, re.DOTALL)
        if i94prtl_match:
            logger.info("Extracting port codes (i94prtl)")
            i94prtl_text = i94prtl_match.group(1)
            i94prtl_dict = {}
            for line in i94prtl_text.strip().split('\n'):
                line = line.strip()
                if '=' in line:
                    code, port = line.split('=', 1)
                    code = code.strip().strip("'")
                    port = port.strip().strip("'").strip()
                    i94prtl_dict[code] = port
            self.lookup_dicts['i94prtl'] = i94prtl_dict

        # Extract i94model format (transportation modes)
        i94model_match = re.search(r'value i94model(.*?);', content, re.DOTALL)
        if i94model_match:
            logger.info("Extracting transport modes (i94model)")
            i94model_text = i94model_match.group(1)
            i94model_dict = {}
            for line in i94model_text.strip().split('\n'):
                line = line.strip()
                if '=' in line:
                    code, mode = line.split('=', 1)
                    code = code.strip()
                    mode = mode.strip().strip("'").strip()
                    try:
                        i94model_dict[int(code)] = mode
                    except ValueError:
                        i94model_dict[code] = mode
            self.lookup_dicts['i94model'] = i94model_dict

        # Extract i94addrl format (state codes)
        i94addrl_match = re.search(r'value i94addrl(.*?);', content, re.DOTALL)
        if i94addrl_match:
            logger.info("Extracting state codes (i94addrl)")
            i94addrl_text = i94addrl_match.group(1)
            i94addrl_dict = {}
            for line in i94addrl_text.strip().split('\n'):
                line = line.strip()
                if '=' in line:
                    code, state = line.split('=', 1)
                    code = code.strip().strip("'")
                    state = state.strip().strip("'").strip()
                    i94addrl_dict[code] = state
            self.lookup_dicts['i94addrl'] = i94addrl_dict

        # Extract visa types if available
        i94visa_match = re.search(r'value i94visa(.*?);', content, re.DOTALL)
        if i94visa_match:
            logger.info("Extracting visa types (i94visa)")
            i94visa_text = i94visa_match.group(1)
            i94visa_dict = {}
            for line in i94visa_text.strip().split('\n'):
                line = line.strip()
                if '=' in line:
                    code, visa = line.split('=', 1)
                    code = code.strip()
                    visa = visa.strip().strip("'").strip()
                    try:
                        i94visa_dict[int(code)] = visa
                    except ValueError:
                        i94visa_dict[code] = visa
            self.lookup_dicts['i94visa'] = i94visa_dict

        logger.info(f"Parsed {len(self.lookup_dicts)} lookup dictionaries")
        return True

    def save_lookups_to_csv(self):
        """Save lookup dictionaries to CSV files."""
        if not self.lookup_dicts:
            logger.error("No lookup dictionaries to save")
            return False

        for name, lookup_dict in self.lookup_dicts.items():
            df = pd.DataFrame(list(lookup_dict.items()), columns=['code', 'description'])
            output_path = os.path.join(self.staging_dir, f"{name}.csv")
            df.to_csv(output_path, index=False)
            logger.info(f"Saved {name} with {len(df)} entries to {output_path}")

        return True

    def run(self):
        """Run the SAS extraction process."""
        logger.info("Starting SAS extraction")

        # Parse the SAS format file
        if not self.parse_sas_format_file():
            logger.error("Failed to parse SAS format file")
            return False

        # Save extracted dictionaries to CSV
        if not self.save_lookups_to_csv():
            logger.error("Failed to save lookup dictionaries")
            return False

        logger.info("SAS extraction completed successfully")
        return True


def main():
    """Main function to run the SAS extractor."""
    # Parameters
    sas_format_path = "backup/data/I94_SAS_Labels_Descriptions.SAS"
    staging_dir = "staging"

    # Create SAS extractor
    extractor = SASExtractor(sas_format_path, staging_dir)

    # Run extractor
    try:
        success = extractor.run()
        if success:
            logger.info("Successfully extracted SAS data to CSV files")
        else:
            logger.error("Failed to extract SAS data")
    except Exception as e:
        logger.error(f"Error in SAS extraction: {e}")


if __name__ == "__main__":
    main()