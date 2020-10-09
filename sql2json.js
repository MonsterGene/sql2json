const fs = require('fs')

fs.readFile('./meter_data.sql', function (err, buffer) {
  if (err) {
    console.log(err)
  } else {
    console.log(err, buffer.length)
    console.log(buffer.indexOf('\n', 0))
    let offset = 0
    let lineEndIndex = buffer.indexOf('\n', offset)
    let tableName
    let headers
    let isFindHeader = false
    let lineNumber = 1
    let cacheData = []
    function readLine () {
      const line = buffer.slice(offset, lineEndIndex).toString()
      offset = lineEndIndex + 1
      lineEndIndex = buffer.indexOf('\n', offset)
      return line
    }
    function saveData (data, next) {
      fs.appendFile(`./meter_data_${Math.floor(lineNumber / 6000)}.json`, data.join(''), function (err) {
        if (!err) {
          console.log(`第${lineNumber}行数据已插入json文件`)
          cacheData = []
          next()
        } else {
          console.log(err)
        }
      })
    }
    function insertData2File() {
      const line = readLine()
      lineNumber++
      // console.log(line)
      if (!isFindHeader) {
        if (line.indexOf('INSERT INTO') !== 0) {
          return insertData2File()
        }
        headers = line.match(/(?:`)([a-zA-Z_]+)(?:`)/g).map(r => r.replace(/`/g, ''))
        tableName = headers.shift()
        isFindHeader = true
        console.log(`Table Name: %c${tableName}%c; Columns: %c${headers}`, 'color: red', '', 'color: yellow')
        insertData2File()
      } else {
        // console.log(/\((.*)\)/.test(line), line)
        if (/\((.*)\)/.test(line)) {
          const start = line.indexOf('(')
          const end = line.indexOf(')')
          const matchRes = line.slice(start + 1, end).replace(/NULL/g, 'null')
          const lineData = matchRes && eval(`[${matchRes}]`)
          // console.log(lineData)
          if (!lineData) return;
          const rowData = headers.reduce((acc, key, n) => {
            acc[key] = lineData[n]
            return acc
          }, {})
          cacheData.push(JSON.stringify(rowData) + '\n')
          if (cacheData.length > 6000) {
            saveData(cacheData, insertData2File)
          } else {
            insertData2File()
          }
        }
      }
    }
    insertData2File()
  }
})