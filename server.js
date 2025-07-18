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

const csv_format = JSON.parse(fs.readFileSync('csv_format.json', 'utf8'));

// Serve HTML form for file submission
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

let results_headers = [];
let results_baskets = [];
let results_payments = [];
let results_products = [];
let results_monetico_retail_remisees = [];
let results_monetico_retail_encours = [];
let results_monetico = [];
let results_conduent = [];
let checkPointMap;
let headerMap;
let conduentMap;

async function process_headers(headerFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(headerFile.path)
      .pipe(csv({ headers: ['orderId', , 'headerEmail', , 'creationDate', "updateDate", , "status", , "headerTotalAmountWithTax", , , , , 'headerImmediateAmountWithTax'], separator: ',' }))
      .on('data', (data) => {

        const { orderId, creationDate, updateDate, status, headerEmail } = data;
        const headerTotalAmountWithTax = Math.round(parseFloat(data.headerTotalAmountWithTax) * 100) / 100;
        const headerImmediateAmountWithTax = Math.round(parseFloat(data.headerImmediateAmountWithTax) * 100) / 100;

        results_headers.push({ orderId, creationDate, updateDate, status, headerTotalAmountWithTax, headerImmediateAmountWithTax, headerEmail });
      })
      .on('end', () => {
        console.log(`Data has been converted and saved`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });
  })
}

async function process_baskets(basketFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(basketFile.path)
      .pipe(csv({ headers: ['orderId', , , 'providerBasketId', , 'providerUserId', 'supportId', , , , , , , , , , , , , , , , , , , , , , 'basketEmail'], separator: ',' }))
      .on('data', (data) => {

        const { orderId, providerBasketId, providerUserId, supportId, basketEmail } = data;

        results_baskets.push({ orderId, providerBasketId, providerUserId, supportId, basketEmail });
      })
      .on('end', () => {
        console.log(`Data has been converted and saved`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });

  })
}

async function process_payments(paymentFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(paymentFile.path)
      .pipe(csv({ headers: ['orderId', , , , "paymentAmountWithTax", "paymentStatus", "paymentRef", "paymentDate", "paymentSEPARef"], separator: ',' }))
      .on('data', (data) => {

        const { orderId, paymentStatus, paymentRef, paymentDate, paymentSEPARef } = data;
        const paymentAmountWithTax = Math.round(parseFloat(data.paymentAmountWithTax) * 100) / 100;
        const findSeparatorforRef = paymentRef.indexOf("$");
        const truncatedPaymentRef = findSeparatorforRef != -1 ? paymentRef.substring(0, findSeparatorforRef) : paymentRef;
        results_payments.push({ orderId, paymentAmountWithTax, paymentStatus, paymentRef, truncatedPaymentRef, paymentDate, paymentSEPARef });
      })
      .on('end', () => {
        console.log(`Data has been converted and saved`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });

  })
}

async function process_products(productFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(productFile.path)
      .pipe(csv({ headers: ['orderId', 'basketLine', , 'productId', , , , , , , , , , , , 'productTotalAmountWithTax', , , , , , , , , , , , , , , , 'productProviderBasketId', 'productImmediateAmountWithTax'], separator: ',' }))
      .on('data', (data) => {

        const { orderId, basketLine, productProviderBasketId, productId } = data;
        const productTotalAmountWithTax = Math.round(parseFloat(data.productTotalAmountWithTax) * 100) / 100;
        const productImmediateAmountWithTax = Math.round(parseFloat(data.productImmediateAmountWithTax) * 100) / 100;
        results_products.push({ orderId, basketLine, productTotalAmountWithTax, productImmediateAmountWithTax, productProviderBasketId, productId });
      })
      .on('end', () => {
        console.log(`Data has been converted and saved`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });

  })
}

async function process_monetico_retail_remisees(moneticoRemiseesFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(moneticoRemiseesFile.path)
      .pipe(csv({ headers: ['orderId', 'type', 'amount', , 'date', , , , , , , 'paymentId'], separator: ';' }))
      .on('data', (data) => {

        const { orderId, type, paymentId } = data;
        const date = data.date.substring(6, 10) + "-" + data.date.substring(3, 5) + "-" + data.date.substring(0, 2) + data.date.substring(10);
        const amount = parseFloat(data.amount);
        results_monetico_retail_remisees.push({ orderId, amount, type, date, paymentId });
      })
      .on('end', () => {
        console.log(`Data has been converted and saved`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });

  })
}

async function process_monetico_retail_encours(moneticoEncoursFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(moneticoEncoursFile.path)
      .pipe(csv({ headers: ['orderId', 'amount', 'status', 'date', , , , , , , , , , , , 'paymentId'], separator: ';' }))
      .on('data', (data) => {

        const { orderId, status, paymentId } = data;
        const date = data.date.substring(6, 10) + "-" + data.date.substring(3, 5) + "-" + data.date.substring(0, 2) + data.date.substring(10);
        const amount = parseFloat(data.amount);
        results_monetico_retail_encours.push({ orderId, amount, status, date, paymentId });
      })
      .on('end', () => {
        console.log(`Data has been converted and saved`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });

  })
}

async function process_conduent(conduentFile) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(conduentFile.path)
      .pipe(csv({ headers: [, 'reference', 'conduentStatus', 'date', 'amount', 'paymentMode', 'userCode', 'email', , , 'IDGCC'], separator: ';' }))
      .on('data', (data) => {

        const { reference, conduentStatus, userCode, paymentMode, email, IDGCC } = data;
        const date = data.date.substring(6, 10) + "-" + data.date.substring(3, 5) + "-" + data.date.substring(0, 2) + data.date.substring(10);
        const amount = parseFloat(data.amount) / 100;
        results_conduent.push({ reference, conduentStatus, date, amount, userCode, paymentMode, email, IDGCC });
      })
      .on('end', () => {
        console.log(`Data has been converted and saved`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });

  })
}

function build_extract(results_headers, results_monetico, results_conduent) {
  try {
    let zipFile = [
      'transactions_nouveaux_rapport.csv'
    ];
    const moneticoPaidStatuses = ['En attente de remise', 'Remisée'];
    const conduentValidatedStatuses = ['ECT'];
    const headerValidatedStatuses = ['VALIDATED', 'FINALIZED'];

    console.log('Début de la construction du fichier des transactions complètes');
    let transactions = [];
    let percentStep = 2;
    let count = 1;
    let checkPoints = [];

    results_monetico_filtered = results_monetico.filter(item => moneticoPaidStatuses.includes(item.status));
    let remaining_results_conduent = results_conduent.filter(item => conduentValidatedStatuses.includes(item.conduentStatus));
    let remaining_results_conduent_map = new Map(
      remaining_results_conduent.map(item => [item.reference, item])
    );

    while (count * percentStep < 100) {
      let index = Math.floor(count * percentStep * results_monetico_filtered.length / 100);
      checkPoints.push({ percent: count * percentStep, paymentRef: results_monetico_filtered[index].paymentId });
      count++;
    }

    checkPointMap = new Map(checkPoints.map((item) => [item.orderId, item.percent]));
    headerMap = new Map(results_headers.map((item) => [item.orderId, item]));
    basketMap = new Map(results_baskets.map((item) => [item.orderId, item]));
    paymentMap = new Map(results_payments.map((item) => [item.paymentRef, item]));
    productMap = new Map(results_products.map((item) => [item.orderId, item]));
    moneticoRetailMap = new Map(results_monetico.map((item) => [item.paymentId, item]));
    conduentMap = new Map(results_conduent.map((item) => [item.reference, item]));


    results_monetico_filtered.forEach(function (item_monetico) {
      const findCheckPoint = checkPointMap.get(item_monetico.paymentId);
      if (findCheckPoint != undefined) {
        console.log("Réalisé: " + findCheckPoint + "%");
      }

      try {

        let result = {
          orderId: item_monetico.orderId,
          paymentId: item_monetico.paymentId,
          moneticoDate: item_monetico.date,
          moneticoAmount: item_monetico.amount,
          moneticoCheck: "OK",
          finalResult: "OK"
        };


        //Récupération des données Conduent
        const conduentMatch = conduentMap.get(item_monetico.paymentId);

        if (conduentMatch != undefined) {
          if (conduentValidatedStatuses.includes(conduentMatch.conduentStatus)) {
            result.conduentAmount = conduentMatch.amount;
            result.conduentDate = conduentMatch.date;
            result.conduentStatus = conduentMatch.conduentStatus;
            result.email = conduentMatch.email;
            result.idgcc = conduentMatch.IDGCC;
            result.paymentMode = conduentMatch.paymentMode;
            result.conduentCheck = "OK";
          } else {
            result.conduentAmount = conduentMatch.amount;
            result.conduentDate = conduentMatch.date;
            result.conduentStatus = conduentMatch.conduentStatus;
            result.email = conduentMatch.email;
            result.idgcc = conduentMatch.IDGCC;
            result.paymentMode = conduentMatch.paymentMode;
            result.conduentCheck = "Mauvais Statut";
            result.finalResult = "KO";
          }
          remaining_results_conduent_map.delete(item_monetico.paymentId);
        } else {
          result.conduentAmount = "Conduent Not Found";
          result.conduentDate = "Conduent Not Found";
          result.conduentStatus = "Conduent Not Found";
          result.email = "Conduent Not Found";
          result.idgcc = "Conduent Not Found";
          result.paymentMode = "Conduent Not Found";
          result.conduentCheck = "Commande introuvable";
          result.finalResult = "KO";
        }


        //Récupération des données du Header
        const headerMatch = headerMap.get(item_monetico.orderId);

        if (headerMatch != undefined) {
          if (headerValidatedStatuses.includes(headerMatch.status)) {
            result.headerStatus = headerMatch.status;
            result.isCheck = "OK";
          } else {
            result.headerStatus = headerMatch.status;
            result.isCheck = "Mauvais Statut";
            result.finalResult = "KO";
          }
        } else {
          result.headerStatus = "IS Not Found";
          result.isCheck = "Commande introuvable";
          result.finalResult = "KO";
        }


        //Récupération des données du basket
        const basketMatch = basketMap.get(item_monetico.orderId);

        if (basketMatch != undefined) {
          result.supportId = basketMatch.supportId;
        } else {
          result.supportId = "IS Not Found";
          result.finalResult = "KO";
        }

        //Récupération des données du product
        const productMatch = Object.assign([], results_products.filter((item) => item.orderId == item_monetico.orderId));

        if (productMatch.length > 0) {
          result.isRegul = productMatch.filter(product => product.productId == "conduent:scheduledpaymentregularisation").length > 0 ? true : false;
        } else {
          result.isRegul = "IS Not Found";
          result.finalResult = "KO";
        }

        transactions.push(result);
      } catch (error) {
        console.error('Something went wrong with orderId:', item_monetico.orderId)
      }
    })

    for (const [key, value] of remaining_results_conduent_map.entries()) {
      if (value.amount != "0") {
        let result = {
          paymentId: value.reference,
          conduentDate: value.date,
          conduentAmount: value.amount,
          conduentStatus: value.conduentStatus,
          email: value.email,
          idgcc: value.IDGCC,
          paymentMode: value.paymentMode,
          conduentCheck: "OK",
          finalResult: "OK"
        }

        //Récupération des données Monetico
        const resultMoneticoFound = moneticoRetailMap.get(value.reference);

        if (resultMoneticoFound != undefined) {
          result.orderId = resultMoneticoFound.orderId;
          result.moneticoDate = resultMoneticoFound.date;
          result.moneticoAmount = resultMoneticoFound.amount;
          result.moneticoCheck = "Mauvais Statut";
          result.finalResult = "KO";
        } else {
          result.orderId = "Monetico Not Found";
          result.moneticoDate = "Monetico Not Found";
          result.moneticoAmount = "Monetico Not Found";
          result.moneticoCheck = "Commande introuvable";
          result.finalResult = "KO";
        }

        //Récupération des données du Payment
        const paymentMatch = paymentMap.get(value.reference);

        let foundOrderId = null;

        if (paymentMatch != undefined) {
          foundOrderId = paymentMatch.orderId;
        }

        //Récupération des données du Header

        //init
        result.headerStatus = "IS Not Found";
        result.isCheck = "Commande introuvable";
        result.finalResult = "KO";

        if (foundOrderId != null) {
          const headerMatch = headerMap.get(foundOrderId);
          if (headerMatch != undefined) {
            if (headerValidatedStatuses.includes(headerMatch.status)) {
              result.headerStatus = headerMatch.status;
              result.isCheck = "OK";
            } else {
              result.headerStatus = headerMatch.status;
              result.isCheck = "Mauvais Statut";
              result.finalResult = "KO";
            }


            //Récupération des données du basket
            const basketMatch = basketMap.get(foundOrderId);

            if (basketMatch != undefined) {
              result.supportId = basketMatch.supportId;
            } else {
              result.supportId = "IS Not Found";
              result.finalResult = "KO";
            }

            //Récupération des données du product
            const productMatch = Object.assign([], results_products.filter((item) => item.orderId == foundOrderId));

            if (productMatch.length > 0) {
              result.isRegul = productMatch.filter(product => product.productId == "conduent:scheduledpaymentregularisation").length > 0 ? true : false;
            } else {
              result.isRegul = "IS Not Found";
              result.finalResult = "KO";
            }

          } else {
            result.headerStatus = "IS Not Found";
            result.isCheck = "Commande introuvable";
            result.finalResult = "KO";
          }
        }

        transactions.push(result);
      }
    }



    fs.writeFileSync(path.join(__dirname, 'outputs', zipFile[0]), build_internal(transactions));

    return zipFile
  } catch (error) {
    console.error('Error filtering successful payments:', error);
  }
}

function build_csv(transactions, headers_labels, orderedAttributes) {
  const rows = transactions.map(function (item) {
    return orderedAttributes.map(header => item[header]).join(';')
  });

  const csvContent = [headers_labels.join(';'), ...rows].join('\n');
  return csvContent;

}

function build_internal(transactions) {
  const result = build_csv(transactions, csv_format.internal.headers_labels, csv_format.internal.orderedAttributes);
  return result

}

function build_stats(usecases) {
  const result = build_csv(usecases, csv_format.stats.headers_labels, csv_format.stats.orderedAttributes);
  return result

}


app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});

app.post('/uploadcsv', upload.fields([
  { name: 'file_header', maxCount: 1 },
  { name: 'file_basket', maxCount: 1 },
  { name: 'file_payment', maxCount: 1 },
  { name: 'file_product', maxCount: 1 },
  { name: 'file_monetico_remisees', maxCount: 1 },
  { name: 'file_monetico_encours', maxCount: 1 },
  { name: 'file_conduent', maxCount: 1 }
]), async (req, res) => {

  const headerFile = req.files.file_header ? req.files.file_header[0] : null;
  const basketFile = req.files.file_basket ? req.files.file_basket[0] : null;
  const paymentFile = req.files.file_payment ? req.files.file_payment[0] : null;
  const productFile = req.files.file_product ? req.files.file_product[0] : null;
  const moneticoRemiseesFile = req.files.file_monetico_remisees ? req.files.file_monetico_remisees[0] : null;
  const moneticoEncoursFile = req.files.file_monetico_encours ? req.files.file_monetico_encours[0] : null;
  const conduentFile = req.files.file_conduent ? req.files.file_conduent[0] : null;
  const requestedstartPaymentDate = req.body.startPaymentDate ? req.body.startPaymentDate : null;
  const requestedendPaymentDate = req.body.endPaymentDate ? req.body.endPaymentDate : null;



  if (headerFile != null && moneticoRemiseesFile != null && moneticoEncoursFile != null && conduentFile != null) {

    try {
      await process_headers(headerFile);
      await process_baskets(basketFile);
      await process_payments(paymentFile);
      await process_products(productFile);
      await process_monetico_retail_remisees(moneticoRemiseesFile);
      await process_monetico_retail_encours(moneticoEncoursFile);
      await process_conduent(conduentFile, results_conduent);

      results_monetico_retail_remisees.splice(0, 1);
      results_monetico_retail_encours.splice(0, 1);

      if (requestedstartPaymentDate != null || requestedendPaymentDate != null) {
        results_monetico_retail_encours = results_monetico_retail_encours.filter((item) => {
          let isValid = true;

          // Then filter by requestedstartPaymentDate
          if (requestedstartPaymentDate != null) {
            isValid = isValid && item.date > requestedstartPaymentDate + ' 00:00:00';
          }

          // Then filter by requestedendPaymentDate
          if (requestedendPaymentDate != null) {
            isValid = isValid && item.date < requestedendPaymentDate + ' 00:00:00';
          }

          return isValid;
        });
      }

      results_monetico = [
        ...results_monetico_retail_remisees.map(item => ({ orderId: item.orderId, status: 'Remisée', amount: item.amount, date: item.date, paymentId: item.paymentId })),
        ...results_monetico_retail_encours.map(item => ({ orderId: item.orderId, status: item.status, amount: item.amount, date: item.date, paymentId: item.paymentId })),
      ];


      results_conduent.splice(0, 1);

      //Filtre des transactions Conduent à partir du 30/06/2025 13:27:00
      console.log(results_conduent.length);
      results_conduent = results_conduent.filter((item) => item.date > '2025-06-30 13:27:00');
      console.log(results_conduent.length);

      const zipFile = build_extract(results_headers, results_monetico, results_conduent);

      const archive = archiver('zip', {
        zlib: { level: 9 }, // Compression level (9 is the maximum)
      });

      archive.pipe(res);

      zipFile.forEach((csvFile) => {
        const filePath = path.join(__dirname, csvFile); // Get the full path of the file
        if (fs.existsSync(filePath)) {
          archive.file(filePath, { name: path.basename(csvFile) });
        }
      });

      archive.on('error', function (err) {
        res.status(500).send({ error: err.message });
      });

      for (let key in req.files) {
        fs.unlink(req.files[key][0].path, (err) => {
          if (err) {
            console.error('Error deleting file:', err);
          } else {
            console.log(`File deleted successfully`);
          }
        });
      }

      res.setHeader('Content-Type', 'application/zip');
      res.setHeader('Content-Disposition', 'attachment; filename=transactions_sytral_prod.zip');
      archive.finalize();

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
    res.status(400).send('Incomplete Files uploaded');
  }
})