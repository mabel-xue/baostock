"""
AkShare数据源适配器
需要安装: pip install akshare
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from .base_datasource import BaseDataSource, DataSourceType

logger = logging.getLogger(__name__)

try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    logger.warning("AkShare未安装，请运行: pip install akshare")


class AkShareDataSource(BaseDataSource):
    """AkShare数据源实现"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.source_type = DataSourceType.AKSHARE
        
        if not AKSHARE_AVAILABLE:
            raise ImportError("AkShare未安装，请运行: pip install akshare")
    
    def connect(self) -> bool:
        """连接到AkShare（AkShare无需登录）"""
        if self._is_connected:
            logger.info("已经连接到AkShare")
            return True
        
        try:
            # AkShare不需要登录，直接标记为已连接
            self._is_connected = True
            logger.info("连接AkShare成功")
            return True
            
        except Exception as e:
            logger.error(f"连接AkShare失败: {str(e)}")
            return False
    
    def disconnect(self) -> bool:
        """断开AkShare连接"""
        if not self._is_connected:
            logger.info("未连接AkShare，无需断开")
            return True
        
        self._is_connected = False
        logger.info("断开AkShare成功")
        return True
    
    def query_balance_sheet(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """查询资产负债表"""
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # AkShare使用纯数字代码
            code = self._extract_code(code)
            
            # AkShare的财务报表接口
            df = ak.stock_financial_report_sina(stock=code, symbol="资产负债表")
            
            # 如果指定了年份，进行过滤
            if year and not df.empty:
                if '报告期' in df.columns:
                    df = df[df['报告期'].str.contains(str(year))]
            
            return df
            
        except Exception as e:
            logger.error(f"查询资产负债表失败: {str(e)}")
            logger.info("提示: AkShare的财务数据接口可能需要特定格式，请参考文档")
            return None
    
    def query_cash_flow(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """查询现金流量表"""
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # AkShare使用纯数字代码
            code = self._extract_code(code)
            
            # AkShare的财务报表接口
            df = ak.stock_financial_report_sina(stock=code, symbol="现金流量表")
            
            # 如果指定了年份，进行过滤
            if year and not df.empty:
                if '报告期' in df.columns:
                    df = df[df['报告期'].str.contains(str(year))]
            
            return df
            
        except Exception as e:
            logger.error(f"查询现金流量表失败: {str(e)}")
            return None
    
    def query_income_statement(
        self,
        code: str,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """查询利润表"""
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # AkShare使用纯数字代码
            code = self._extract_code(code)
            
            # AkShare的财务报表接口
            df = ak.stock_financial_report_sina(stock=code, symbol="利润表")
            
            # 如果指定了年份，进行过滤
            if year and not df.empty:
                if '报告期' in df.columns:
                    df = df[df['报告期'].str.contains(str(year))]
            
            return df
            
        except Exception as e:
            logger.error(f"查询利润表失败: {str(e)}")
            return None
    
    def query_stock_basic(self, **kwargs) -> Optional[pd.DataFrame]:
        """查询股票基本信息"""
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # 获取A股股票列表
            df = ak.stock_info_a_code_name()
            return df
            
        except Exception as e:
            logger.error(f"查询股票基本信息失败: {str(e)}")
            return None
    
    def query_daily_data(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """查询日线行情数据"""
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # AkShare使用纯数字代码
            code = self._extract_code(code)
            
            # 默认日期范围
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            else:
                end_date = end_date.replace('-', '')
            
            if not start_date:
                start_date = '19900101'  # AkShare可以获取全部历史数据
            else:
                start_date = start_date.replace('-', '')
            
            # 获取历史行情数据
            adjust = kwargs.get('adjust', 'qfq')  # qfq=前复权, hfq=后复权, ''=不复权
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            return df
            
        except Exception as e:
            logger.error(f"查询日线数据失败: {str(e)}")
            return None
    
    def normalize_code(self, code: str) -> str:
        """
        标准化股票代码为AkShare格式
        AkShare格式: 纯数字代码，如 600000 或 000001
        """
        return self._extract_code(code)
    
    def _extract_code(self, code: str) -> str:
        """
        提取纯数字代码
        
        Args:
            code: 原始代码（可能包含市场前缀或后缀）
            
        Returns:
            str: 纯数字代码
        """
        code = code.strip()
        
        # 移除各种前缀和后缀
        code = code.replace('sh.', '').replace('sz.', '')
        code = code.replace('SH.', '').replace('SZ.', '')
        code = code.replace('.SH', '').replace('.SZ', '')
        code = code.replace('.sh', '').replace('.sz', '')
        
        return code
    
    def query_financial_indicator(
        self,
        code: str,
        year: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询财务指标
        这是AkShare特有的接口
        
        Args:
            code: 股票代码
            year: 年份
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 财务指标数据
        """
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            code = self._extract_code(code)
            
            # 获取财务指标
            df = ak.stock_financial_analysis_indicator(symbol=code)
            
            # 如果指定了年份，进行过滤
            if year and not df.empty:
                if '报告期' in df.columns:
                    df = df[df['报告期'].str.contains(str(year))]
            
            return df
            
        except Exception as e:
            logger.error(f"查询财务指标失败: {str(e)}")
            return None
    
    def query_fund_basic(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        查询基金基本信息
        
        根据AkShare官方文档，使用 fund_open_fund_daily_em() 获取所有开放式基金的实时净值信息
        返回包含：基金代码、简称、单位净值、累计净值、日增长率、申购赎回状态等
        
        Args:
            **kwargs: 查询参数
                - fund_type: 基金类型（可选，暂未使用）
                
        Returns:
            Optional[pd.DataFrame]: 基金基本信息DataFrame
        """
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # 根据官方文档，使用 fund_open_fund_daily_em() 获取所有开放式基金的实时净值信息
            # 返回包含基金代码、简称、单位净值、累计净值等信息
            df = ak.fund_open_fund_daily_em()
            return df
            
        except AttributeError as e:
            logger.error(f"接口不存在: {str(e)}")
            logger.info("提示: 请确保已安装最新版本的akshare: pip install --upgrade akshare")
            # 尝试备用接口
            try:
                logger.info("尝试使用备用接口 fund_em_open_fund_daily...")
                df = ak.fund_em_open_fund_daily()
                return df
            except AttributeError:
                logger.error("备用接口也不存在，请检查akshare版本")
                return None
        except Exception as e:
            logger.error(f"查询基金基本信息失败: {str(e)}")
            return None
    
    def query_fund_holdings(
        self,
        fund_code: str,
        period: Optional[str] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询基金持仓明细
        
        根据AkShare官方文档，使用 fund_portfolio_hold_em(symbol, date) 查询基金持仓
        返回包含：股票代码、占净值百分比、持股数、持仓市值、所属季度等信息
        
        Args:
            fund_code: 基金代码（6位数字），如 "000001"
            period: 报告期，支持格式：
                - 年份：如 "2024" - 返回该年份所有季度的数据
                - 季度：如 "2024Q3" - 返回该年该季度的数据（会先查询全年数据，然后过滤）
                - 日期：如 "20240331" - 自动提取年份和季度，返回该季度的数据
                不指定则使用当前年份减1
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 基金持仓数据DataFrame
                如果指定了季度，返回该季度的数据；如果只指定年份，返回该年份所有季度的数据
        """
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # 根据官方文档，使用 fund_portfolio_hold_em(symbol, date)
            # symbol: 基金代码，date: 年份（如 "2024"）
            # 注意：接口返回该年份所有季度的数据，需要通过"季度"字段过滤
            
            # 处理period参数，提取年份和季度信息
            target_year = None
            target_quarter = None
            
            if period:
                # 如果period是季度格式 "2024Q3"，提取年份和季度
                if 'Q' in str(period).upper():
                    parts = str(period).upper().split('Q')
                    target_year = parts[0]
                    target_quarter = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
                # 如果是日期格式 "20240331"，提取年份和季度
                elif len(str(period)) == 8:
                    target_year = str(period)[:4]
                    month = int(str(period)[4:6])
                    target_quarter = (month - 1) // 3 + 1
                # 如果只是年份格式 "2024"
                elif len(str(period)) == 4:
                    target_year = str(period)
                else:
                    target_year = str(period)
            else:
                # 默认使用当前年份减1
                target_year = str(datetime.now().year - 1)
            
            # 优先使用官方文档中的接口
            try:
                logger.info(f"使用官方接口 fund_portfolio_hold_em(symbol={fund_code}, date={target_year})")
                df = ak.fund_portfolio_hold_em(symbol=fund_code, date=target_year)
                logger.info(f"基金 {fund_code} 持仓数据列名: {list(df.columns)}")
            except AttributeError:
                # 尝试备用接口名称
                try:
                    logger.info("尝试使用备用接口 fund_em_portfolio_hold...")
                    df = ak.fund_em_portfolio_hold(fund=fund_code, period=target_year)
                except AttributeError:
                    logger.error("未找到基金持仓查询接口，请检查akshare版本")
                    return None
            except KeyError as e:
                # 基金可能没有持仓数据（如新基金、货币基金等）
                logger.warning(f"基金 {fund_code} 没有持仓数据或数据格式不符: {str(e)}")
                return pd.DataFrame()
            
            if df is None or df.empty:
                return pd.DataFrame()
            
            # 如果指定了季度，根据"季度"字段过滤数据
            if target_quarter is not None:
                # 查找季度列（可能的列名：季度、所属季度、报告期等）
                quarter_col = None
                for col in ['季度', '所属季度', '报告期', 'period']:
                    if col in df.columns:
                        quarter_col = col
                        break
                
                if quarter_col:
                    # 季度字段格式通常是 "2024年1季度股票投资明细" 或类似格式
                    # 匹配格式：年份 + "年" + 季度数字 + "季度"
                    quarter_pattern = f"{target_year}年{target_quarter}季度"
                    df = df[df[quarter_col].str.contains(quarter_pattern, na=False, regex=False)]
                    logger.info(f"过滤 {target_year}年第{target_quarter}季度数据，剩余 {len(df)} 条")
                else:
                    logger.warning(f"未找到季度列，无法过滤季度数据，返回全年数据。可用列: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"查询基金持仓失败: {str(e)}")
            return None
    
    def query_fund_nav(
        self,
        fund_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询基金净值走势
        
        根据AkShare官方文档，使用 fund_open_fund_info_em(symbol, indicator, period) 查询基金净值
        返回包含：日期、单位净值、累计净值等时间序列数据
        
        Args:
            fund_code: 基金代码（6位数字），如 "000001"
            start_date: 开始日期，格式：YYYY-MM-DD 或 YYYYMMDD，不指定则查询最近一年
            end_date: 结束日期，格式：YYYY-MM-DD 或 YYYYMMDD，不指定则使用当前日期
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 基金净值数据DataFrame，已按日期范围过滤
        """
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # 默认日期范围
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            else:
                end_date = end_date.replace('-', '')
            
            if not start_date:
                # 默认查询最近一年
                start_date = (datetime.now().replace(year=datetime.now().year-1)).strftime('%Y%m%d')
            else:
                start_date = start_date.replace('-', '')
            
            # 根据官方文档，使用 fund_open_fund_info_em(symbol, indicator, period)
            # symbol: 基金代码，indicator: 指标类型，period: 时间范围
            df = None
            try:
                # 使用官方文档中的正确接口
                df = ak.fund_open_fund_info_em(
                    symbol=fund_code, 
                    indicator="单位净值走势",
                    period="成立来"  # 获取全部历史数据，后续再过滤日期
                )
            except AttributeError:
                # 尝试备用接口名称
                try:
                    logger.info("尝试使用备用接口 fund_em_open_fund_info...")
                    df = ak.fund_em_open_fund_info(fund=fund_code, indicator="单位净值走势")
                except AttributeError:
                    logger.error("未找到基金净值查询接口，请检查akshare版本")
                    logger.info("提示: 请确保已安装最新版本的akshare: pip install --upgrade akshare")
                    logger.info("提示: 正确接口应为 fund_open_fund_info_em(symbol, indicator, period)")
                    return None
            
            if df is None or df.empty:
                return pd.DataFrame()
            
            # 根据返回数据的列名进行日期过滤
            # 可能的列名：净值日期、日期、date、净值日期等
            date_column = None
            for col in ['净值日期', '日期', 'date', '净值日期']:
                if col in df.columns:
                    date_column = col
                    break
            
            if date_column:
                try:
                    df[date_column] = pd.to_datetime(df[date_column])
                    start_dt = pd.to_datetime(start_date)
                    end_dt = pd.to_datetime(end_date)
                    df = df[(df[date_column] >= start_dt) & (df[date_column] <= end_dt)]
                except Exception as e:
                    logger.warning(f"日期过滤失败: {str(e)}，返回全部数据")
            else:
                logger.warning("未找到日期列，返回全部数据")
            
            return df
            
        except Exception as e:
            logger.error(f"查询基金净值失败: {str(e)}")
            logger.info("提示: 请确保已安装最新版本的akshare: pip install --upgrade akshare")
            return None
    
    def query_institutional_holdings(
        self,
        stock_code: Optional[str] = None,
        period: Optional[str] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询机构持仓明细
        
        Args:
            stock_code: 股票代码（可选，不提供则查询所有机构持仓）
            period: 报告期（格式：2024Q3，可选）
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 机构持仓数据
        """
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            if stock_code:
                # 查询特定股票的机构持仓
                code = self._extract_code(stock_code)
                df = ak.stock_institute_hold_detail(symbol=code, period=period)
            else:
                # 如果没有指定股票代码，返回空（AkShare需要指定股票代码）
                logger.warning("查询机构持仓需要指定股票代码")
                return None
            
            return df
            
        except Exception as e:
            logger.error(f"查询机构持仓失败: {str(e)}")
            return None
    
    def query_fund_rank(
        self,
        top_n: int = 100,
        sort_by: str = "近1年",
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询基金排名（开放式基金）
        
        根据AkShare官方文档，使用 fund_open_fund_rank_em(symbol) 查询基金排名
        返回包含：基金代码、基金简称、单位净值、日增长率、近1周、近1月、近3月、近6月、近1年、近2年、近3年等收益率
        
        Args:
            top_n: 返回前N个基金，默认100
            sort_by: 排序字段，支持以下选项：
                - "近1周" - 按近1周收益率排序
                - "近1月" - 按近1月收益率排序
                - "近3月" - 按近3月收益率排序
                - "近6月" - 按近6月收益率排序
                - "近1年" - 按近1年收益率排序（默认）
                - "近2年" - 按近2年收益率排序
                - "近3年" - 按近3年收益率排序
                - "今年来" - 按今年以来收益率排序
                - "成立来" - 按成立以来收益率排序
            **kwargs: 其他参数
                - fund_type: 基金类型，如 "全部"、"股票型"、"混合型"、"债券型"等
            
        Returns:
            Optional[pd.DataFrame]: 基金排名数据DataFrame，按指定字段降序排列
            
        Examples:
            >>> # 查询近1年收益率前50的基金
            >>> df = datasource.query_fund_rank(top_n=50, sort_by="近1年")
            
            >>> # 查询近2年收益率前100的股票型基金
            >>> df = datasource.query_fund_rank(top_n=100, sort_by="近2年", fund_type="股票型")
        """
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # 获取基金类型参数，默认为"全部"
            fund_type = kwargs.get('fund_type', '全部')
            
            # 根据官方文档，使用 fund_open_fund_rank_em(symbol)
            # symbol: 基金类型，如 "全部"、"股票型"、"混合型"、"债券型"等
            logger.info(f"查询基金排名: 类型={fund_type}, 排序字段={sort_by}, 前{top_n}个")
            df = ak.fund_open_fund_rank_em(symbol=fund_type)
            
            if df is None or df.empty:
                logger.warning(f"未查询到基金排名数据")
                return pd.DataFrame()
            
            logger.info(f"查询到 {len(df)} 条基金数据，列名: {list(df.columns)}")
            
            # 检查排序字段是否存在
            if sort_by not in df.columns:
                logger.warning(f"排序字段 '{sort_by}' 不存在于数据中，可用字段: {list(df.columns)}")
                logger.warning(f"将返回原始数据的前{top_n}条")
                return df.head(top_n)
            
            # 按指定字段降序排序，并返回前N个
            # 需要先转换为数值类型（某些字段可能是字符串格式，如 "10.5%"）
            try:
                # 尝试转换为数值类型
                df_sorted = df.copy()
                if df_sorted[sort_by].dtype == 'object':
                    # 如果是字符串类型，尝试移除百分号并转换
                    df_sorted[sort_by] = df_sorted[sort_by].astype(str).str.replace('%', '').replace('---', '0').replace('', '0')
                    df_sorted[sort_by] = pd.to_numeric(df_sorted[sort_by], errors='coerce')
                
                # 按指定字段降序排序
                df_sorted = df_sorted.sort_values(by=sort_by, ascending=False, na_position='last')
                result = df_sorted.head(top_n)
                
                logger.info(f"成功返回按 '{sort_by}' 排序的前{top_n}个基金")
                return result
                
            except Exception as e:
                logger.warning(f"排序失败: {str(e)}，返回原始数据的前{top_n}条")
                return df.head(top_n)
            
        except AttributeError as e:
            logger.error(f"接口不存在: {str(e)}")
            logger.info("提示: 请确保已安装最新版本的akshare: pip install --upgrade akshare")
            return None
        except Exception as e:
            logger.error(f"查询基金排名失败: {str(e)}")
            return None
    
    def query_fund_scale(
        self,
        top_n: int = 100,
        fund_type: str = "股票型基金",
        sort_by: str = "最近总份额",
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        查询基金规模排名（开放式基金）
        
        根据AkShare官方文档，使用 fund_scale_open_sina(symbol) 查询基金规模
        返回包含：基金代码、基金简称、单位净值、总募集规模、最近总份额、成立日期、基金经理等
        
        Args:
            top_n: 返回前N个基金，默认100
            fund_type: 基金类型，支持以下选项：
                - "股票型基金" - 股票型基金（默认）
                - "混合型基金" - 混合型基金
                - "债券型基金" - 债券型基金
                - "货币型基金" - 货币型基金
                - "QDII基金" - QDII基金
            sort_by: 排序字段，支持以下选项：
                - "最近总份额" - 按最近总份额排序（默认，代表当前规模）
                - "总募集规模" - 按总募集规模排序（成立时募集规模）
            **kwargs: 其他参数
            
        Returns:
            Optional[pd.DataFrame]: 基金规模数据DataFrame，按指定字段降序排列
            
        Examples:
            >>> # 查询规模前50的股票型基金
            >>> df = datasource.query_fund_scale(top_n=50, fund_type="股票型基金")
            
            >>> # 查询规模前100的混合型基金
            >>> df = datasource.query_fund_scale(top_n=100, fund_type="混合型基金")
        """
        if not self._is_connected:
            logger.error("未连接到AkShare")
            return None
        
        try:
            # 根据官方文档，使用 fund_scale_open_sina(symbol)
            # symbol: 基金类型，如 "股票型基金"、"混合型基金"、"债券型基金"等
            logger.info(f"查询基金规模: 类型={fund_type}, 排序字段={sort_by}, 前{top_n}个")
            df = ak.fund_scale_open_sina(symbol=fund_type)
            
            if df is None or df.empty:
                logger.warning(f"未查询到基金规模数据")
                return pd.DataFrame()
            
            logger.info(f"查询到 {len(df)} 条基金规模数据，列名: {list(df.columns)}")
            
            # 检查排序字段是否存在
            if sort_by not in df.columns:
                logger.warning(f"排序字段 '{sort_by}' 不存在于数据中，可用字段: {list(df.columns)}")
                logger.warning(f"将返回原始数据的前{top_n}条")
                return df.head(top_n)
            
            # 按指定字段降序排序，并返回前N个
            try:
                # 尝试转换为数值类型
                df_sorted = df.copy()
                if df_sorted[sort_by].dtype == 'object':
                    # 如果是字符串类型，尝试转换
                    df_sorted[sort_by] = pd.to_numeric(df_sorted[sort_by], errors='coerce')
                
                # 按指定字段降序排序
                df_sorted = df_sorted.sort_values(by=sort_by, ascending=False, na_position='last')
                result = df_sorted.head(top_n)
                
                logger.info(f"成功返回按 '{sort_by}' 排序的前{top_n}个基金")
                return result
                
            except Exception as e:
                logger.warning(f"排序失败: {str(e)}，返回原始数据的前{top_n}条")
                return df.head(top_n)
            
        except AttributeError as e:
            logger.error(f"接口不存在: {str(e)}")
            logger.info("提示: 请确保已安装最新版本的akshare: pip install --upgrade akshare")
            logger.info("提示: 正确接口应为 fund_scale_open_sina(symbol)")
            return None
        except Exception as e:
            logger.error(f"查询基金规模失败: {str(e)}")
            return None