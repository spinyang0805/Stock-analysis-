// 借貸試算的未來預估（蒙地卡羅 + 歷史全月份摘要）在背景執行緒計算，避免卡住畫面
import { computeFuture } from "./loanModel.js";

self.onmessage = (e) => {
  const { id, p, months, tr, seed, paths } = e.data;
  const r = computeFuture(p, months, tr, seed, paths);
  self.postMessage({ id, mc: r.mc, sumry: r.sumry });
};
