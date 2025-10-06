const fs = require('fs');
const csv = require('csv-parser');

const express = require('express');
const multer = require('multer');
const archiver = require('archiver');
const path = require('path');
const app = express();
const port = process.env.PORT || 8080;

const uploadDir = path.join(__dirname, 'uploads');
const storage = multer.diskStorage({
  destination: uploadDir,
  filename: (req, file, cb) => {
    cb(null, file.originalname);
  }
});
const upload = multer({ storage: storage });


const results_transactions = [];
let date;

// Serve HTML form for file submission
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index_extract_figures.html'));
});



async function process_file_v1(transactionFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(transactionFile.path)
      .pipe(csv({ headers: [, , , , , , , , , , , , , , , , , , , , 'payment_date', , , , , , , , , , , , , , "case1", "case2", "case3", "case4", "case5", "case6", "case7", "case8", "case9", "case10"], separator: ';' }))
      .on('data', (data) => {

        const { payment_date, case1, case2, case3, case4, case5, case6, case7, case8, case9, case10 } = data;
        results_transactions.push({ payment_date, case1, case2, case3, case4, case5, case6, case7, case8, case9, case10 });
      })
      .on('end', () => {
        console.log(`Fichier headers traité avec succès`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });
  })
}

async function process_file_v2(transactionFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(transactionFile.path)
      .pipe(csv({ headers: [, , "payment_date", , , , , "statut_script"], separator: ';' }))
      .on('data', (data) => {

        const { payment_date, statut_script } = data;
        let isKO = false;
        if (statut_script != "Monetico OK / Conduent OK") {
          isKO = true;
        }
        results_transactions.push({ payment_date, isKO });
      })
      .on('end', () => {
        console.log(`Fichier headers traité avec succès`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });
  })
}

async function process_file_v3(transactionFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(transactionFile.path)
      .pipe(csv({ headers: [, , "payment_date", , , , , , , , , , , , , , , "statut_script"], separator: ';' }))
      .on('data', (data) => {

        const { payment_date, statut_script } = data;
        let isKO = false;
        if (statut_script != "OK") {
          isKO = true;
        }
        results_transactions.push({ payment_date, isKO });
      })
      .on('end', () => {
        console.log(`Fichier headers traité avec succès`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });
  })
}

async function process_file_v4(transactionFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(transactionFile.path)
      .pipe(csv({ headers: [, , "payment_date", , , , , , , , , , , , , , , , "statut_script"], separator: ';' }))
      .on('data', (data) => {

        const { payment_date, statut_script } = data;
        let isKO = false;
        if (statut_script != "OK") {
          isKO = true;
        }
        results_transactions.push({ payment_date, isKO });
      })
      .on('end', () => {
        console.log(`Fichier headers traité avec succès`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });
  })
}

async function process_file_v5(transactionFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(transactionFile.path)
      .pipe(csv({ headers: [, , "payment_date_monetico_raw", , "payment_date_conduent_raw", , , , , , , , , , , , , , "statut_script"], separator: ';' }))
      .on('data', (data) => {

        const { payment_date_monetico_raw, payment_date_conduent_raw, statut_script } = data;
        const payment_date = payment_date_monetico_raw ? payment_date_monetico_raw : payment_date_conduent_raw;

        let isKO = true;
        if (["OK - Produit distribué et payé par CB", "OK - Produit distribué et payé par SEPA"].includes(statut_script)) {
          isKO = false;
        }
        results_transactions.push({ payment_date, isKO });
      })
      .on('end', () => {
        console.log(`Fichier headers traité avec succès`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });
  })
}

async function process_file_v6(transactionFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(transactionFile.path)
      .pipe(csv({ headers: [, , "payment_date_monetico_raw", , "payment_date_conduent_raw", , , , , , , , , , , , , , , "statut_script"], separator: ';' }))
      .on('data', (data) => {

        const { payment_date_monetico_raw, payment_date_conduent_raw, statut_script } = data;
        const payment_date = payment_date_monetico_raw ? payment_date_monetico_raw : payment_date_conduent_raw;

        let isKO = true;
        if (["OK - Produit distribué et payé par CB", "OK - Produit distribué et payé par SEPA"].includes(statut_script)) {
          isKO = false;
        }
        results_transactions.push({ payment_date, isKO });
      })
      .on('end', () => {
        console.log(`Fichier headers traité avec succès`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });
  })
}

function convertDate(str) {
  // str = "01/07/2025 00:00"
  const [datePart, timePart] = str.split(' ');
  const [day, month, year] = datePart.split('/');
  // Pad time to include seconds if missing
  const time = timePart.length === 5 ? timePart + ':00' : timePart;
  return `${year}-${month}-${day} ${time}`;
}


async function build_extract() {
  try {
    let content = fs.readFileSync('outputs/metrics.csv', 'utf8');
    let minDate = null;
    let maxDate = null;
    let totalKOTransactions = 0;

    results_transactions.splice(0, 1);

    results_transactions.forEach(item => {
      if (item.payment_date > maxDate || maxDate == null) {
        maxDate = item.payment_date;
      }
      if (item.payment_date < minDate || minDate == null) {
        minDate = item.payment_date;
      }
      if (item.isKO) {
        totalKOTransactions = totalKOTransactions + 1;
      }
    });

    content += `\n${date};${minDate};${maxDate};${results_transactions.length};${totalKOTransactions}`;


    fs.writeFileSync('outputs/metrics.csv', content, { encoding: 'utf8' });

  } catch (error) {
    console.error('Error filtering successful payments:', error);
  }
}


app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});

app.post('/uploadcsv', upload.fields([
  { name: 'file_transactions', maxCount: 1 }
]), async (req, res) => {

  const transactionFile = req.files.file_transactions ? req.files.file_transactions[0] : null;
  date = req.body.date ? req.body.date : null;

  if (transactionFile != null && date != null) {

    try {
      results_transactions.length = 0;
      if (date <= "2025-07-08") {
        await process_file_v1(transactionFile);
      } else if (date <= "2025-07-16") {
        await process_file_v2(transactionFile);
      } else if (date <= "2025-07-21") {
        await process_file_v3(transactionFile);
      } else if (date <= "2025-07-23") {
        await process_file_v4(transactionFile);
      } else if (date <= "2025-09-21") {
        await process_file_v5(transactionFile);
      } else if (date <= "2025-12-21") {
        await process_file_v6(transactionFile);
      }

      await build_extract();




      for (let key in req.files) {
        fs.unlink(req.files[key][0].path, (err) => {
          if (err) {
            console.error('Error deleting file:', err);
          } else {
            console.log(`File deleted successfully`);
          }
        });
      }

      res.status(200).send('OK - DONE');

    } catch (error) {
      for (let key in req.files) {
        fs.unlink(req.files[key][0].path, (err) => {
          if (err) {
            console.error('Error deleting file:', err);
          } else {
            console.log(`File deleted successfully`);
          }
        });
      }

      res.status(500).send('Error processing the uploaded file');
    }
  } else {
    res.status(400).send('Manque des données');
  }
})