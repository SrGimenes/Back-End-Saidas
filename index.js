let fs = require('fs');
let ExcelReader = require('node-excel-stream').ExcelReader;
let axios = require('axios');

async function readExcel() {
  let dataStream = fs.createReadStream('Base.xlsx');
  let reader = new ExcelReader(dataStream, {
    sheets: [{
      name: 'Plan1',
      rows: {
        headerRow: 1,
        allowedHeaders: [
          {
            name: 'CNPJ_REVENDA',
            key: 'cnpjRevenda',
          },
          {
            name: 'HASH_REVENDA',
            key: 'hashRevenda'
          },
          {
            name: 'NR_NF',
            key: 'nrNf',
          },
          {
            name: 'SERIE_NF',
            key: 'serieNf',
          },
          {
            name: 'DT_NF',
            key: 'dtNf',
          },
          {
            name: 'COD_TIPO_PRODUTO',
            key: 'codTipoProduto',
          },
          {
            name: 'CPF_CNPJ_DESTINO',
            key: 'cpfCnpjDestino',
          },
          {
            name: 'NOME_DESTINO',
            key: 'nomeDestino',
          },
          {
            name: 'COD_MUNICIPIO_DESTINO',
            key: 'codMunicipioDestino',
          },
          {
            name: 'COD_LANCAMENTO_USUARIO',
            key: 'codLancamentoUsuario',
          },
          {
            name: 'COD_CULTIVAR',
            key: 'codCultivar',
          },
          {
            name: 'LOTE',
            key: 'lote',
          },
          {
            name: 'QNT_PRODUTO',
            key: 'qntProduto',
          }
        ]
      }
    }]
  })
  // fs.appendFileSync('log.json', '');
  console.log('starting parse');

  let data = [];

  reader.eachRow((rowData, rowNum, sheetSchema) => { // Cada linha que le o arquivo, ele chama a função aqui dentro
    data.push(rowData)
  })
    .then(() => { // Só para saber que terminou de ler
      console.log(data[0])
      registrarDados(data);
      console.log('done parsing');
    });
}

async function readJson() {
  fs.readFile('log.json', 'utf-8', async function (err, data) {
    const json = JSON.parse(data);
    await fs.unlink('log.json', async function (err, data) { }) // Utilizado para apagar o arquivo json para depois reutiliza - lo
    for await (row of json) {
      await registrarDados(row);
    }
  });
}

async function registrarDados(rowData) {
  let formatedData = []

  for (let i = 0; i < rowData.length; i++) {
    let filteredData = rowData.filter(data => data.nrNf == rowData[i].nrNf);
    let itens = [];

    // MESCLA TODOS OS ITENS DA MESMA NOTA FISCAL
    for (let i = 0; i < filteredData.length; i++) {
      itens.push({
        codCultivar: filteredData[i].codCultivar,
        lote: filteredData[i].lote,
        qntProduto: filteredData[i].qntProduto
      })
    }

    let cnpjRevenda = new String(filteredData[0].cnpjRevenda);
    let cpfCnpjDestino = new String(filteredData[0].cpfCnpjDestino);

    // FAZ UNIÃO DE TODOS OS DADOS
    let dataMixed = {
      cnpjRevenda: cnpjRevenda.length !== 14 ? `0${cnpjRevenda}` : `${cnpjRevenda}`,
      hashRevenda: filteredData[0].hashRevenda,
      destinoArmazenador: false,
      nrNf: filteredData[0].nrNf,
      serieNf: filteredData[0].serieNf,
      dtNf: new Intl.DateTimeFormat("fr-CA", { year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(filteredData[0].dtNf)),
      destinoRevenda: false,
      codTipoProduto: filteredData[0].codTipoProduto,
      renasemDestino: "",
      nomeDestino: "teste",
      cpfCnpjDestino: cpfCnpjDestino.length <= 11 ?
        (cpfCnpjDestino.length < 11 ? `0${cpfCnpjDestino}` : `${cpfCnpjDestino}`)
        :
        (cpfCnpjDestino.length < 14 ? `0${cpfCnpjDestino}` : `${cpfCnpjDestino}`),
      codMunicipioDestino: filteredData[0].codMunicipioDestino,
      codLancamentoUsuario: filteredData[0].codLancamentoUsuario,
      itens
    }

    formatedData.push(dataMixed);
  }

  // FAZ UMA BUSCA POR ITENS DUPLICADOS PELO NÚMERO DA NOTA FISCAL 
  const cleanData = [...new Map(formatedData.map((item, key) => [item['nrNf'], item])).values()]
  var errors = [];
  var success = [];
  for (let i = 0; i < cleanData.length; i++) {
    try {
      const response = await axios.post('https://vegetal.indea.mt.gov.br/SISDEV/ws/saidaSementesMudas', cleanData[i], {
        auth: {
          username: '',
          password: ''
        }
      })
      success.push({
        message: response.data,
        data: cleanData[i]
      })
    } catch (e) {
      errors.push({
        message: e.data,
        data: cleanData[i]
      })
    }
  }
  if (errors.length != 0) {
    fs.appendFileSync('log-400.json', `${JSON.stringify(errors)}`);
  }
  if (success.length != 0) {
    fs.appendFileSync('log-200.json', `${JSON.stringify(success)}`);
  }
}

  async function start() {
    await readExcel()
    //await readJson();
  }

  start()
