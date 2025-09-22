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
let results_monetico_retail_remisees_selected = [];
let results_monetico_retail_encours_selected = [];
let results_monetico = [];
let results_monetico_selected = [];
let results_conduent = [];
let results_conduent_selected = [];
let checkPointMap;
let headerMap;
let conduentMap;
let remaining_results_conduent;
const moneticoPaidStatuses = ['En attente de remise', 'Remisée'];
const conduentValidatedStatuses = ['ECT'];
const headerValidatedStatuses = ['VALIDATED', 'FINALIZED', 'PAYMENT_CONFIRMED'];
let transactions = [];

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
        console.log(`Fichier headers traité avec succès`);
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
        console.log(`Fichier baskets traité avec succès`);
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
        console.log(`Fichier payments traité avec succès`);
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
        console.log(`Fichier products traité avec succès`);
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
      .pipe(csv({ headers: ['orderId', 'type', 'amount', , 'date', , , , , , 'moneticoEmail', 'paymentId'], separator: ';' }))
      .on('data', (data) => {

        const { orderId, type, paymentId, moneticoEmail } = data;
        const date = data.date.substring(6, 10) + "-" + data.date.substring(3, 5) + "-" + data.date.substring(0, 2) + data.date.substring(10);
        const amount = parseFloat(data.amount);
        results_monetico_retail_remisees.push({ orderId, amount, type, date, paymentId, moneticoEmail });
      })
      .on('end', () => {
        console.log(`Fichier monetico_remisees traité avec succès`);
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
      .pipe(csv({ headers: ['orderId', 'amount', 'status', 'date', , , , , , 'type', , , , , 'moneticoEmail', 'paymentId'], separator: ';' }))
      .on('data', (data) => {

        const { orderId, status, paymentId, type, moneticoEmail } = data;
        const date = data.date.substring(6, 10) + "-" + data.date.substring(3, 5) + "-" + data.date.substring(0, 2) + data.date.substring(10);
        const amount = parseFloat(data.amount);
        results_monetico_retail_encours.push({ orderId, amount, status, date, paymentId, type, moneticoEmail });
      })
      .on('end', () => {
        console.log(`Fichier monetico_encours traité avec succès`);
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

        if (results_conduent.find(item => item.reference === data.reference) == undefined) {
          const { reference, conduentStatus, userCode, paymentMode, email, IDGCC } = data;
          const date = data.date.substring(6, 10) + "-" + data.date.substring(3, 5) + "-" + data.date.substring(0, 2) + data.date.substring(10);
          const amount = parseFloat(data.amount) / 100;
          results_conduent.push({ reference, conduentStatus, date, amount, userCode, paymentMode, email, IDGCC });
        }
      })
      .on('end', () => {
        console.log(`Fichier conduent traité avec succès`);
        resolve(); // Resolve the promise when the reading is complete
      })
      .on('error', (error) => {
        console.error('Error reading the CSV file:', error);
        reject(error); // Reject the promise if there's an error
      });

  })
}

function build_extract(results_headers, results_monetico, results_monetico_selected, results_conduent, results_conduent_selected) {
  try {
    let zipFile = [
      'transactions_nouveaux_rapport.csv'
    ];


    console.log('Début de la construction du fichier des transactions complètes');
    let percentStep = 2;
    let count = 1;
    let checkPoints = [];

    results_monetico_filtered = results_monetico_selected.filter(item => moneticoPaidStatuses.includes(item.status));
    remaining_results_conduent = results_conduent_selected.filter(item => conduentValidatedStatuses.includes(item.conduentStatus));

    while (count * percentStep < 100) {
      let index = Math.floor(count * percentStep * results_monetico_filtered.length / 100);
      checkPoints.push({ percent: count * percentStep, paymentRef: results_monetico_filtered[index].paymentId });
      count++;
    }

    checkPointMap = new Map(checkPoints.map((item) => [item.paymentRef, item.percent]));
    headerMap = new Map(results_headers.map((item) => [item.orderId, item]));
    basketMap = new Map(results_baskets.map((item) => [item.orderId, item]));
    paymentMap = new Map(results_payments.map((item) => [item.paymentRef, item]));
    productMap = new Map(results_products.map((item) => [item.orderId, item]));
    moneticoRetailMap = new Map(results_monetico.map((item) => [item.paymentId, item]));
    conduentMap = new Map(results_conduent.map((item) => [item.reference, item]));


    const creditTransactions = results_monetico_filtered.filter(item => item.type == "Crédit");
    creditTransactionsMap = new Map(creditTransactions.map((item) => [item.orderId, item]));

    results_monetico_filtered.forEach(function (item_monetico) {
      const findCheckPoint = checkPointMap.get(item_monetico.paymentId);
      if (findCheckPoint != undefined) {
        console.log("Réalisé: " + findCheckPoint + "%");
      }

      if (item_monetico.type == "Débit") {
        try {
          const creditFound = creditTransactionsMap.get(item_monetico.orderId);
          if (creditFound == undefined) {
            checkSources("debit", item_monetico);
          } else {
            checkSources("credit", item_monetico);
          }
        } catch (error) {
          console.error('Something went wrong with orderId:', item_monetico.orderId)
        }
      }
    })

    remaining_results_conduent.forEach(transaction => {
      if ((transaction.amount != "0") && (transaction.paymentMode == "CB")) {
        let result = {
          paymentId: transaction.reference,
          conduentDate: transaction.date,
          conduentAmount: transaction.amount,
          conduentStatus: transaction.conduentStatus,
          email: transaction.email,
          idgcc: transaction.IDGCC,
          paymentMode: transaction.paymentMode,
          isRefund: false,
          conduentCheck: "OK"
        }

        //Récupération des données Monetico
        const resultMoneticoFound = moneticoRetailMap.get(transaction.reference);

        if (resultMoneticoFound != undefined) {
          result.orderId = resultMoneticoFound.orderId;
          result.moneticoDate = resultMoneticoFound.date;
          result.moneticoAmount = resultMoneticoFound.amount;
          result.moneticoStatus = resultMoneticoFound.status;
          result.moneticoEmail = resultMoneticoFound.moneticoEmail;
          result.moneticoCheck = "Mauvais Statut";
        } else {
          result.orderId = "Monetico Not Found";
          result.moneticoDate = "Monetico Not Found";
          result.moneticoAmount = "Monetico Not Found";
          result.moneticoStatus = "Monetico Not Found";
          result.moneticoEmail = "Monetico Not Found";
          result.moneticoCheck = "Commande introuvable";
        }

        //Récupération des données du Payment
        const paymentMatch = paymentMap.get(transaction.reference);

        let foundOrderId = null;

        if (paymentMatch != undefined) {
          foundOrderId = paymentMatch.orderId;
        }

        //Récupération des données du Header

        //init
        result.headerStatus = "IS Not Found";
        result.isCheck = "Commande introuvable";

        if (foundOrderId != null) {
          const headerMatch = headerMap.get(foundOrderId);
          if (headerMatch != undefined) {
            result.orderId = headerMatch.orderId;
            if (headerValidatedStatuses.includes(headerMatch.status)) {
              result.headerStatus = headerMatch.status;
              result.isCheck = "OK";
            } else {
              result.headerStatus = headerMatch.status;
              result.isCheck = "Mauvais Statut";
            }


            //Récupération des données du basket
            const basketMatch = basketMap.get(foundOrderId);

            if (basketMatch != undefined) {
              result.supportId = basketMatch.supportId;
            } else {
              result.supportId = "IS Not Found";
            }

            //Récupération des données du product
            const productMatch = Object.assign([], results_products.filter((item) => item.orderId == foundOrderId));

            if (productMatch.length > 0) {
              result.isRegul = productMatch.filter(product => product.productId == "conduent:scheduledpaymentregularisation").length > 0 ? true : false;
            } else {
              result.isRegul = "IS Not Found";
            }

          } else {
            result.headerStatus = "IS Not Found";
            result.isCheck = "Commande introuvable";
          }
        }

        transactions.push(convertNumber(result));
      } else if (transaction.paymentMode != "CB") {
        let result = {
          paymentId: transaction.reference,
          conduentDate: transaction.date,
          conduentAmount: transaction.amount,
          conduentStatus: transaction.conduentStatus,
          email: transaction.email,
          idgcc: transaction.IDGCC,
          paymentMode: transaction.paymentMode,
          conduentCheck: "OK"
        }
        transactions.push(convertNumber(result));
      }
    })

    transactions.map(transaction => {
      if (transaction.isRefund == false) {
        if (transaction.moneticoCheck == "OK") {
          if (transaction.conduentCheck == "OK") {
            transaction.finalResult = "OK - Produit distribué et payé par CB";
          } else if (transaction.conduentCheck == "Mauvais Statut") {
            transaction.finalResult = "KO - Produit payé par CB mais pas distribué";
          } else if (transaction.conduentCheck == "Commande introuvable") {
            transaction.finalResult = "KO - Produit payé par CB mais commande introuvable";
          }
        } else if (transaction.moneticoCheck == "Mauvais Statut") {
          if (transaction.conduentCheck == "OK") {
            transaction.finalResult = "KO - Produit distribué mais pas payé";
          } else if (transaction.conduentCheck == "Mauvais Statut") {
            transaction.finalResult = "OK - Produit pas distribué et pas payé";
          } else if (transaction.conduentCheck == "Commande introuvable") {
            transaction.finalResult = "OK - Produit pas payé et commande introuvable";
          }
        } else {
          if ((transaction.paymentMode == "Autre") && (transaction.conduentCheck == "OK")) {
            transaction.finalResult = "OK - Produit distribué et payé par SEPA";
          } else {
            transaction.finalResult = "KO - Produit distribué mais pas payé";
          }
        }
      } else {
        if (transaction.moneticoCheck == "OK") {
          if (transaction.conduentCheck == "OK") {
            transaction.finalResult = "KO - Paiement remboursé mais produit distribué quand même";
          } else if (transaction.conduentCheck == "Mauvais Statut") {
            transaction.finalResult = "OK - Paiement remboursé et produit non distribué";
          } else if (transaction.conduentCheck == "Commande introuvable") {
            transaction.finalResult = "OK - Paiement remboursé et pas de commande";
          }
        } else if (transaction.moneticoCheck == "Mauvais Statut") {
          if (transaction.conduentCheck == "OK") {
            transaction.finalResult = "KO - Produit distribué mais pas payé";
          } else if (transaction.conduentCheck == "Mauvais Statut") {
            transaction.finalResult = "OK - Produit pas distribué et pas payé";
          } else if (transaction.conduentCheck == "Commande introuvable") {
            transaction.finalResult = "OK - Produit pas payé et commande introuvable";
          }
        } else {
          if ((transaction.paymentMode == "Autre") && (transaction.conduentCheck == "OK")) {
            transaction.finalResult = "OK - Produit distribué et payé par SEPA";
          } else {
            transaction.finalResult = "KO - Produit distribué mais pas payé";
          }
        }
      }
    })

    fs.writeFileSync(path.join(__dirname, 'outputs', zipFile[0]), '\uFEFF' + build_internal(transactions), { encoding: 'utf8' });

    return zipFile
  } catch (error) {
    console.error('Error filtering successful payments:', error);
  }
}

function checkSources(type, item_monetico) {
  let result = {
    orderId: item_monetico.orderId,
    paymentId: item_monetico.paymentId,
    moneticoDate: item_monetico.date,
    moneticoAmount: item_monetico.amount,
    moneticoStatus: item_monetico.status,
    moneticoEmail: item_monetico.moneticoEmail,
    moneticoCheck: "OK",
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
    }
    remaining_results_conduent = remaining_results_conduent.filter(item => item.reference != item_monetico.paymentId);
  } else {
    result.conduentAmount = "Conduent Not Found";
    result.conduentDate = "Conduent Not Found";
    result.conduentStatus = "Conduent Not Found";
    result.email = "Conduent Not Found";
    result.idgcc = "Conduent Not Found";
    result.paymentMode = "Conduent Not Found";
    result.conduentCheck = "Commande introuvable";
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
    }
  } else {
    result.headerStatus = "IS Not Found";
    result.isCheck = "Commande introuvable";
  }


  //Récupération des données du basket
  const basketMatch = basketMap.get(item_monetico.orderId);

  if (basketMatch != undefined) {
    result.supportId = basketMatch.supportId;
  } else {
    result.supportId = "IS Not Found";
  }

  //Récupération des données du product
  const productMatch = Object.assign([], results_products.filter((item) => item.orderId == item_monetico.orderId));

  if (productMatch.length > 0) {
    result.isRegul = productMatch.filter(product => product.productId == "conduent:scheduledpaymentregularisation").length > 0 ? true : false;
  } else {
    result.isRegul = "IS Not Found";
  }

  transactions.push(convertNumber(result));

  if (type == "debit") {
    result.isRefund = false;

  } else if (type == "credit") {
    result.isRefund = true;
  }
}

function convertNumber(transaction) {
  if (transaction.conduentAmount) {
    transaction.conduentAmount = transaction.conduentAmount.toString().replace('.', ',');
  }
  if (transaction.moneticoAmount) {
    transaction.moneticoAmount = transaction.moneticoAmount.toString().replace('.', ',');
  }
  return transaction;
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



  if (headerFile != null && moneticoRemiseesFile != null && conduentFile != null) {

    try {
      await process_headers(headerFile);
      await process_baskets(basketFile);
      await process_payments(paymentFile);
      await process_products(productFile);
      await process_monetico_retail_remisees(moneticoRemiseesFile);
      //await process_monetico_retail_encours(moneticoEncoursFile);
      await process_conduent(conduentFile, results_conduent);

      results_monetico_retail_remisees.splice(0, 1);
      //results_monetico_retail_encours.splice(0, 1);
      results_conduent.splice(0, 1);



      if (requestedstartPaymentDate != null || requestedendPaymentDate != null) {
        /*results_monetico_retail_encours_selected = results_monetico_retail_encours.filter((item) => {
          let isValid = true;

          // Then filter by requestedstartPaymentDate
          if (requestedstartPaymentDate != null) {
            isValid = isValid && item.date > requestedstartPaymentDate + ' 00:00:00';
          }

          // Then filter by requestedendPaymentDate
          if (requestedendPaymentDate != null) {
            isValid = isValid && item.date < requestedendPaymentDate + ' 00:00:00';
          }

          if (results_monetico_retail_remisees.find(item2 => item2.paymentId == item.paymentId)) {
            isValid = false
          }

          return isValid;
        });*/

        results_monetico_retail_remisees_selected = results_monetico_retail_remisees.filter((item) => {
          let isValid = true;

          // Then filter by requestedstartPaymentDate
          if (requestedstartPaymentDate != null) {
            isValid = isValid && item.date > requestedstartPaymentDate + ' 00:00:00';
          }

          // Then filter by requestedendPaymentDate
          if (requestedendPaymentDate != null) {
            isValid = isValid && item.date <= requestedendPaymentDate + ' 00:00:00';
          }

          return isValid;
        });

        results_conduent_selected = results_conduent.filter((item) => {
          let isValid = true;

          // Then filter by requestedstartPaymentDate
          if (requestedstartPaymentDate != null) {
            isValid = isValid && item.date > requestedstartPaymentDate + ' 00:00:00';
          } else {//Filtre des transactions Conduent à partir du 30/06/2025 13:27:00
            isValid = isValid && item.date > requestedstartPaymentDate + '2025-06-30 13:27:00';
          }

          // Then filter by requestedendPaymentDate
          if (requestedendPaymentDate != null) {
            isValid = isValid && item.date < requestedendPaymentDate + ' 00:00:00';
          }

          return isValid;
        });

      }

      console.log("Transactions Monetico Remisées: " + results_monetico_retail_remisees.length + " --> " + results_monetico_retail_remisees_selected.length);
      //console.log("Fichier initial de Monetico Encours: " + results_monetico_retail_encours.length + " --> " + results_monetico_retail_encours_selected.length);
      console.log("Fichier initial de Conduent: " + results_conduent.length + " --> " + results_conduent_selected.length);

      results_monetico = [
        ...results_monetico_retail_remisees.map(item => ({ orderId: item.orderId, status: 'Remisée', moneticoEmail: item.moneticoEmail, amount: item.amount, date: item.date, paymentId: item.paymentId, type: item.type }))
        //...results_monetico_retail_encours.map(item => ({ orderId: item.orderId, status: item.status, moneticoEmail: item.moneticoEmail, amount: item.amount, date: item.date, paymentId: item.paymentId, type: item.type })),
      ];

      results_monetico_selected = [
        ...results_monetico_retail_remisees_selected.map(item => ({ orderId: item.orderId, status: 'Remisée', moneticoEmail: item.moneticoEmail, amount: item.amount, date: item.date, paymentId: item.paymentId, type: item.type }))
        //...results_monetico_retail_encours_selected.map(item => ({ orderId: item.orderId, status: item.status, moneticoEmail: item.moneticoEmail, amount: item.amount, date: item.date, paymentId: item.paymentId, type: item.type })),
      ];


      const zipFile = build_extract(results_headers, results_monetico, results_monetico_selected, results_conduent, results_conduent_selected);

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