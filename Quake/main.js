const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const os = require('os');
const fs = require('fs');

const CFG = path.join(app.getPath('userData'), 'autostart.json');
function readCfg(){ try { return JSON.parse(fs.readFileSync(CFG,'utf8')); } catch(e){ return {auto:false}; } }
function writeCfg(o){ try { fs.writeFileSync(CFG, JSON.stringify(o)); } catch(e){} }

function createWindow() {
  const win = new BrowserWindow({
    width: 1280, height: 820,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false,
      webSecurity: false,
      allowRunningInsecureContent: true
    }
  });
  win.setMenuBarVisibility(false);
  win.loadFile('quake.html');
}

ipcMain.handle('autostart:get', () => readCfg().auto);
ipcMain.handle('autostart:set', (e, on) => {
  const o = readCfg(); o.auto = !!on; writeCfg(o);
  try { app.setLoginItemSettings({ openAtLogin: !!on, path: process.execPath }); } catch(err){}
  return o.auto;
});

// 文件选择对话框，默认打开下载目录
ipcMain.handle('pick-file', async () => {
  const result = await dialog.showOpenDialog({
    defaultPath: path.join(os.homedir(), 'Downloads'),
    properties: ['openFile'],
    filters: [{ name: '音频', extensions: ['mp3','wav','ogg','m4a','aac'] }]
  });
  if (result.canceled || !result.filePaths.length) return null;
  return result.filePaths[0];
});

app.whenReady().then(() => {
  const cfg = readCfg();
  if (cfg.auto){
    try { app.setLoginItemSettings({ openAtLogin: true, path: process.execPath }); } catch(e){}
  }
  createWindow();
});

app.on('window-all-closed', () => app.quit());
