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

async function process_headers(headerFile, results_headers) {
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


async function process_baskets(basketFile, results_baskets) {
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

async function process_payments(paymentFile, results_payments) {
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

async function process_products(productFile, results_products) {
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

async function process_monetico(moneticoFile, results_monetico) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(moneticoFile.path)
      .pipe(csv({ headers: ['tpe', , 'reference', 'date', , 'amount', , 'moneticoStatus'], separator: ';' }))
      .on('data', (data) => {

        const { tpe, reference, date, moneticoStatus } = data;
        const amount = parseFloat(data.amount);
        const findSeparatorforRef = reference.indexOf("$");
        const truncatedPaymentRef = findSeparatorforRef != -1 ? reference.substring(0, findSeparatorforRef) : reference;
        results_monetico.push({ tpe, reference, date, amount, moneticoStatus, truncatedPaymentRef });
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

async function process_conduent(conduentFile, results_conduent) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(conduentFile.path)
      .pipe(csv({ headers: [, 'reference', 'conduentStatus', , 'amount', 'paymentMode', 'userCode', 'email', , , 'IDGCC'], separator: ';' }))
      .on('data', (data) => {

        const { reference, conduentStatus, userCode, paymentMode, email, IDGCC } = data;
        const amount = parseFloat(data.amount) / 100;
        const findSeparatorforRef = reference.indexOf("$");
        const truncatedReference = findSeparatorforRef != -1 ? reference.substring(0, findSeparatorforRef) : reference;
        results_conduent.push({ reference, truncatedReference, conduentStatus, amount, userCode, paymentMode, email, IDGCC });
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

function build_extract(results_payments, results_headers, results_baskets, results_products, results_monetico, results_conduent, completeFileToProcess, callbackKOFileToProcess) {

  let zipFile = [
    'transactions_completes.csv',
    'transactions_callbackKO.csv'
  ];

  if (completeFileToProcess) {
    console.log('Début de la construction du fichier des transactions complètes');
    let transactions = [];
    let percentStep = 2;
    let count = 1;
    let checkPoints = [];
    while (count * percentStep < 100) {
      let index = Math.floor(count * percentStep * results_payments.length / 100);
      checkPoints.push({ percent: count * percentStep, orderId: results_payments[index].orderId });
      count++;
    }

    results_payments.forEach(function (result_payment) {
      let findCheckPoint = checkPoints.findIndex((item) => item.orderId == result_payment.orderId);
      if (findCheckPoint != -1) {
        console.log("Réalisé: " + checkPoints[findCheckPoint].percent + "%");
        checkPoints.splice(findCheckPoint, 1);
      }

      let multiplePaymentElements = false;
      if (results_payments.filter((item) => item.orderId == result_payment.orderId).length > 1) {
        multiplePaymentElements = true;
      }
      const header_attributes = Object.assign({}, results_headers.find((item) => item.orderId == result_payment.orderId));
      if ((result_payment.paymentStatus == "SUCCESS") || (result_payment.paymentStatus == "STARTED" && header_attributes.status == "PAYMENT_PROCESSING")) {

        try {
          const basket_attributes = Object.assign({}, results_baskets.find((item) => item.orderId == result_payment.orderId));
          const product_attributes = Object.assign([], results_products.filter((item) => item.orderId == result_payment.orderId));

          const sumTotalBaskets = product_attributes.reduce((acc, product) => acc + parseFloat(product.productTotalAmountWithTax), 0).toFixed(2);
          const sumImmediateBaskets = product_attributes.reduce((acc, product) => acc + parseFloat(product.productImmediateAmountWithTax), 0).toFixed(2);
          const internalTotalCheck = sumTotalBaskets == header_attributes.headerTotalAmountWithTax;
          const internalImmediateCheck = sumImmediateBaskets == result_payment.paymentAmountWithTax;
          const containsPaymentRegularisation = product_attributes.filter(product => product.productId == "conduent:scheduledpaymentregularisation").length > 0;
          let moneticoImmediateCheck;
          let conduentTotalCheck;
          let moneticoStatus;
          let moneticoAmount;
          let conduentStatus;
          let conduentAmount;


          const indexMoneticoFound = results_monetico.findIndex(item => item.reference == result_payment.paymentRef);

          if (indexMoneticoFound != -1) {
            const monetico_attributes = Object.assign({}, results_monetico[indexMoneticoFound]);
            moneticoImmediateCheck = sumImmediateBaskets == monetico_attributes.amount
            moneticoAmount = monetico_attributes.amount;
            moneticoStatus = monetico_attributes.moneticoStatus;
            moneticoTPE = monetico_attributes.tpe;
            moneticoReference = monetico_attributes.reference
          } else {
            const allMoneticoResultsFound = results_monetico.filter(item => item.truncatedPaymentRef == header_attributes.orderId && item.moneticoStatus != "EN" && item.moneticoStatus != "RE");
            if (allMoneticoResultsFound.length > 0) {
              const monetico_attributes = Object.assign({}, allMoneticoResultsFound[0]);
              moneticoImmediateCheck = sumImmediateBaskets == monetico_attributes.amount
              moneticoAmount = monetico_attributes.amount;
              moneticoStatus = monetico_attributes.moneticoStatus;
              moneticoTPE = monetico_attributes.tpe;
              moneticoReference = allMoneticoResultsFound.map(result=>result.reference).join("---");
            } else {
              moneticoImmediateCheck = "Monetico Not Found";
              moneticoStatus = "Monetico Not Found";
              moneticoAmount = "Monetico Not Found";
              moneticoTPE = "Monetico Not Found";
              moneticoReference = "Monetico Not Found";
            }
          }


          if (result_payment.paymentRef != 'NULL') {
            const indexConduentFoundwithMonetico = results_conduent.findIndex(item => item.truncatedReference == result_payment.truncatedPaymentRef);
            if (indexConduentFoundwithMonetico != -1) {
              const conduent_attributes = Object.assign({}, results_conduent[indexConduentFoundwithMonetico]);
              conduentTotalCheck = header_attributes.headerTotalAmountWithTax == conduent_attributes.amount;
              conduentAmount = conduent_attributes.amount;
              conduentStatus = conduent_attributes.conduentStatus;
              conduentPaymentMode = conduent_attributes.paymentMode;
              conduentTransactionNumber = conduent_attributes.reference;
              conduentIDGCC = conduent_attributes.IDGCC;
              conduentEmail = conduent_attributes.email;
            } else {
              const indexConduentFoundwithSEPA = results_conduent.findIndex(item => item.IDGCC == basket_attributes.providerBasketId && item.userCode == basket_attributes.providerUserId);
              if (indexConduentFoundwithSEPA != -1) {
                const conduent_attributes = Object.assign({}, results_conduent[indexConduentFoundwithSEPA]);
                conduentTotalCheck = header_attributes.headerTotalAmountWithTax == conduent_attributes.amount;
                conduentAmount = conduent_attributes.amount;
                conduentStatus = conduent_attributes.conduentStatus;
                conduentPaymentMode = conduent_attributes.paymentMode;
                conduentTransactionNumber = conduent_attributes.reference;
                conduentIDGCC = conduent_attributes.IDGCC;
                conduentEmail = conduent_attributes.email;
              } else {
                conduentTotalCheck = "Conduent Not Found";
                conduentStatus = "Conduent Not Found";
                conduentAmount = "Conduent Not Found";
                conduentPaymentMode = "Conduent Not Found";
                conduentTransactionNumber = "Conduent Not Found";
                conduentIDGCC = "Conduent Not Found";
                conduentEmail = "Conduent Not Found";
              }
            }
          } else {
            const indexConduentFoundwithSEPA = results_conduent.findIndex(item => item.paymentMode == "Autre" && item.IDGCC == basket_attributes.providerBasketId);
            if (indexConduentFoundwithSEPA != -1) {
              const conduent_attributes = Object.assign({}, results_conduent[indexConduentFoundwithSEPA]);
              conduentTotalCheck = header_attributes.headerTotalAmountWithTax == conduent_attributes.amount;
              conduentAmount = conduent_attributes.amount;
              conduentStatus = conduent_attributes.conduentStatus;
              conduentPaymentMode = conduent_attributes.paymentMode;
              conduentTransactionNumber = conduent_attributes.reference;
              conduentIDGCC = conduent_attributes.IDGCC;
              conduentEmail = conduent_attributes.email;
            } else {
              conduentTotalCheck = "Conduent Not Found";
              conduentStatus = "Conduent Not Found";
              conduentAmount = "Conduent Not Found";
              conduentPaymentMode = "Conduent Not Found";
              conduentTransactionNumber = "Conduent Not Found";
              conduentIDGCC = "Conduent Not Found";
              conduentEmail = "Conduent Not Found";
            }
          }

          let duplicatePayment = false;
          const duplicatePaymentTransactionsIndex = transactions
            .map((item, index) => item.orderId == result_payment.orderId ? index : -1)
            .filter(index => index != -1);
          if (duplicatePaymentTransactionsIndex.length > 0) {
            duplicatePaymentTransactionsIndex.forEach(function (transactionIndex) {
              transactions[transactionIndex].duplicatePayment = true;
            })
            duplicatePayment = true;
          }

          if ((header_attributes) && (basket_attributes)) {
            delete header_attributes.orderId;
            delete basket_attributes.orderId;

            //case1 = (conduentStatus == "Conduent Not Found" && moneticoStatus == "Monetico Not Found");
            //case2 = (header_attributes.status == 'VALIDATION_ERROR' && conduentStatus != 'Conduent Not Found' && moneticoStatus != 'Monetico Not Found');
            //case4 = (header_attributes.status == 'VALIDATION_ERROR' && conduentStatus == 'Conduent Not Found' && moneticoStatus != 'Monetico Not Found' && moneticoStatus != 'RE');
            //case5 = (header_attributes.status == 'FINALIZED' && conduentStatus == 'Conduent Not Found' && moneticoStatus != 'Monetico Not Found' && moneticoStatus != 'RE');
            //case6 = (header_attributes.status != 'VALIDATION_ERROR' && header_attributes.status != 'FINALIZED' && conduentStatus == 'Conduent Not Found' && moneticoStatus != 'Monetico Not Found' && moneticoStatus != 'RE');
            //case9 = (conduentStatus != 'Conduent Not Found' && moneticoImmediateCheck == false && internalTotalCheck == false);
            //case10 = (conduentStatus == 'Conduent Not Found' && moneticoImmediateCheck == false && internalTotalCheck == false);

            transactions.push({ ...result_payment, ...header_attributes, ...basket_attributes, containsPaymentRegularisation, sumTotalBaskets, sumImmediateBaskets, internalTotalCheck, internalImmediateCheck, moneticoImmediateCheck, conduentTotalCheck, duplicatePayment, moneticoStatus, moneticoTPE, moneticoAmount, moneticoReference, conduentStatus, conduentIDGCC, conduentTransactionNumber, conduentPaymentMode, conduentAmount, conduentEmail, multiplePaymentElements })
            if (indexMoneticoFound != -1) {
              results_monetico.splice(indexMoneticoFound, 1);
            }
          } else {
            console.error('Data missing for orderId:', result_payment.orderId)
          }
        } catch (error) {
          console.error('Something went wrong with orderId:', result_payment.orderId)
        }
      }
    })

    const transactionsWithCase = transactions.map(transaction => ({
      ...transaction,
      newCase1: (transaction.paymentStatus == "SUCCESS" && transaction.duplicatePayment == true && transaction.conduentStatus != 'Conduent Not Found' && transaction.conduentPaymentMode == 'CB') ? true : false,
      newCase2: (transaction.paymentStatus == "SUCCESS" && transaction.duplicatePayment == true && transaction.conduentStatus != 'Conduent Not Found' && transaction.conduentPaymentMode == 'Autre') ? true : false,
      newCase3: (transaction.paymentStatus == "SUCCESS" && transaction.duplicatePayment == true && transaction.conduentStatus == 'Conduent Not Found') ? true : false,
      newCase4: (transaction.paymentStatus == "SUCCESS" && transaction.duplicatePayment == false && transaction.moneticoStatus != 'Monetico Not Found' && transaction.moneticoStatus != 'RE' && transaction.conduentStatus != 'Conduent Not Found' && transaction.conduentStatus != 'ANN' && transaction.status == 'VALIDATION_ERROR') ? true : false,
      newCase5: (transaction.paymentStatus == "SUCCESS" && transaction.duplicatePayment == false && transaction.moneticoStatus != 'Monetico Not Found' && transaction.moneticoStatus != 'RE' && transaction.conduentStatus != 'Conduent Not Found' && transaction.status != 'VALIDATION_ERROR' && transaction.conduentTotalCheck == false) ? true : false,
      newCase6: (transaction.paymentStatus == "SUCCESS" && transaction.duplicatePayment == false && transaction.paymentRef != "NULL" && transaction.moneticoStatus != 'Monetico Not Found' && transaction.moneticoStatus != 'RE' && transaction.conduentStatus != 'Conduent Not Found' && transaction.status != 'VALIDATION_ERROR' && transaction.conduentPaymentMode == 'Autre') ? true : false,
      newCase7: (transaction.paymentStatus == "SUCCESS" && transaction.duplicatePayment == false && transaction.moneticoStatus != 'Monetico Not Found' && transaction.moneticoStatus != 'RE' && transaction.conduentStatus == 'Conduent Not Found' && transaction.status == 'VALIDATION_ERROR') ? true : false,
      newCase8: (transaction.paymentStatus == "SUCCESS" && transaction.duplicatePayment == false && transaction.moneticoStatus != 'Monetico Not Found' && transaction.moneticoStatus != 'RE' && transaction.conduentStatus == 'Conduent Not Found' && transaction.status != 'VALIDATION_ERROR') ? true : false,
      newCase9: (transaction.paymentStatus == "SUCCESS" && transaction.duplicatePayment == false && transaction.moneticoStatus == 'Monetico Not Found' && transaction.moneticoStatus != 'RE' && transaction.conduentStatus != 'Conduent Not Found' && transaction.conduentPaymentMode == 'CB') ? true : false,
      newCase10: (transaction.status == "PAYMENT_PROCESSING" && transaction.moneticoStatus != 'Monetico Not Found' && transaction.moneticoStatus != 'EN' && transaction.moneticoStatus != 'RE') ? true : false,
    }));

    fs.writeFileSync(path.join(__dirname, zipFile[0]), build_internal(transactionsWithCase));
  }

  if (callbackKOFileToProcess) {
    console.log('Début de la construction du fichier des callbacks KO');
    let moneticoTransactions = [];
    results_monetico.shift();

    let percentStep = 2;
    let count = 1;
    let checkPoints = [];
    while (count * percentStep < 100) {
      let index = Math.floor(count * percentStep * results_monetico.length / 100);
      checkPoints.push({ percent: count * percentStep, reference: results_monetico[index].reference });
      count++;
    }



    results_monetico.forEach(function (result_monetico) {
      let findCheckPoint = checkPoints.findIndex((item) => item.reference == result_monetico.reference);
      if (findCheckPoint != -1) {
        console.log("Réalisé: " + checkPoints[findCheckPoint].percent + "%");
        checkPoints.splice(findCheckPoint, 1);
      }
      if (result_monetico.moneticoStatus != "RE" && result_monetico.moneticoStatus != "EN") {
        const findSeparatorforRef = result_monetico.reference.indexOf("$");
        const truncatedPaymentRef = findSeparatorforRef != -1 ? result_monetico.reference.substring(0, findSeparatorforRef) : result_monetico.reference;
        const foundHeader = results_headers.find((item) => item.orderId == truncatedPaymentRef);
        const ref = result_monetico.reference;
        const moneticoStatus = result_monetico.moneticoStatus;
        const date = result_monetico.date;
        if (foundHeader) {
          const { orderId, status } = foundHeader;
          if (status == "PAYMENT_PROCESSING") {
            moneticoTransactions.push({ orderId, status, ref, moneticoStatus, date });
          }
        } else {
          moneticoTransactions.push({ 'orderId': 'NULL', 'status': 'NULL', ref, moneticoStatus, date });
        }
      }
    })

    fs.writeFileSync(path.join(__dirname, zipFile[1]), build_callbackKO(moneticoTransactions));
  }

  return zipFile

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

function build_callbackKO(transactions) {
  const result = build_csv(transactions, csv_format.callbackKO.headers_labels, csv_format.callbackKO.orderedAttributes);
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
  { name: 'file_monetico', maxCount: 1 },
  { name: 'file_conduent', maxCount: 1 }
]), async (req, res) => {

  const headerFile = req.files.file_header ? req.files.file_header[0] : null;
  const basketFile = req.files.file_basket ? req.files.file_basket[0] : null;
  const paymentFile = req.files.file_payment ? req.files.file_payment[0] : null;
  const productFile = req.files.file_product ? req.files.file_product[0] : null;
  const moneticoFile = req.files.file_monetico ? req.files.file_monetico[0] : null;
  const conduentFile = req.files.file_conduent ? req.files.file_conduent[0] : null;
  const requestedOrderId = req.body.orderId ? req.body.orderId : null;
  const requestedpaymentDate = req.body.paymentDate ? req.body.paymentDate : null;
  const completeFileToProcess = req.body.complete ? req.body.complete : null;
  const callbackKOFileToProcess = req.body.callbackko ? req.body.callbackko : null;



  if (headerFile != null && basketFile != null && paymentFile != null && productFile != null) {

    const results_headers = [];
    const results_baskets = [];
    const results_payments = [];
    const results_products = [];
    const results_monetico = [];
    const results_conduent = [];
    let requested_payments = [];
    //const outputFile2Name = 'reste_monetico.csv';
    //const outputFile2Path = path.join(__dirname, outputFile2Name);

    try {
      await process_headers(headerFile, results_headers);
      await process_baskets(basketFile, results_baskets);
      await process_payments(paymentFile, results_payments);
      await process_products(productFile, results_products);

      if (moneticoFile != null) {
        await process_monetico(moneticoFile, results_monetico);
      }
      if (conduentFile != null) {
        await process_conduent(conduentFile, results_conduent);
      }

      if (requestedOrderId != null || requestedpaymentDate != null) {
        requested_payments = results_payments.filter((item) => {
          let isValid = true;

          // First filter by requestedOrderId
          if (requestedOrderId != null) {
            isValid = isValid && item.orderId == requestedOrderId;
          }

          // Then filter by requestedpaymentDate
          if (requestedpaymentDate != null) {
            isValid = isValid && item.paymentDate < requestedpaymentDate + ' 00:00:00';
          }
          return isValid;
        });
      } else {
        requested_payments = results_payments;
      }


      const zipFile = build_extract(requested_payments, results_headers, results_baskets, results_products, results_monetico, results_conduent, completeFileToProcess, callbackKOFileToProcess);

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