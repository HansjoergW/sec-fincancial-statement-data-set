"""Contains the definitions to standardize incaome statements."""
from typing import List

from secfsdstools.f_standardize.base_prepivot_rules import PrePivotDeduplicate, PrePivotCorrectSign
from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.base_rules import CopyTagRule, SumUpRule, SubtractFromRule
from secfsdstools.f_standardize.base_validation_rules import ValidationRule
from secfsdstools.f_standardize.standardizing import Standardizer


class IncomeStatementStandardizer(Standardizer):
    """
    The goal of this Standardizer is to create IncomeStatements that are comparable,
    meaning that they have the same tags.

    At the end, the standardized IS contains the following columns

    Position	Relation

    Revenues
           + SalesRevenueGoodsNet
           + SalesRevenueServicesNet
           + OtherSalesRevenueNet
           --------------
        or SalesRevenueNet

        or RevenuesSum
              RevenueFromContractWithCustomerExcludingAssessedTax
              RevenueFromContractWithCustomerIncludingAssessedTax
              RevenuesExcludingInterestAndDividends
              RegulatedAndUnregulatedOperatingRevenue
              ...

        or InterestAndDividendIncomeOperating
        --------
        Revenues
        ========

    CostOfRevenue
          'CostOfGoodsSold',
        + 'CostOfServices'
        ---------------
          'CostOfGoodsAndServicesSold',
        + 'LicenseCost',
        ----------------
          'CostOfRevenue',
        ================


    Gross Profit	                Revenues - Cost of Goods and Services Sold
    Operating Expenses

    Operating Income (Loss)	        Gross Profit - Operating Expenses
    Other Income and Expenses

    Income Before Tax               Operating Income + Other Income and Expenses
    Income Tax Expense (Benefit)

    Net Income (Loss)               Income Before Tax - Income Tax Expense
    """
    prepivot_rule_tree = RuleGroup(
        prefix="IS_PREPIV",
        rules=[PrePivotDeduplicate(),
               PrePivotCorrectSign(
                   tag_list=['CostOfGoodsSold', 'CostOfServices',
                             'CostOfGoodsAndServicesSold', 'LicenseCost',
                             'CostOfRevenue'],
                   is_positive=True
               )]
    )

    is_cogs_sumup_rg = RuleGroup(
        prefix="IS_COGSUMUP",
        rules=[
            SumUpRule(sum_tag='CostOfGoodsAndServicesSold',
                      potential_summands=['CostOfGoodsSold', 'CostOfServices']),
            SumUpRule(sum_tag='CostOfRevenue',
                      potential_summands=['CostOfGoodsAndServicesSold', 'LicenseCost'])
        ]
    )

    is_revenue_rg = RuleGroup(
        prefix="Rev",
        rules=[
            SumUpRule(sum_tag='SalesRevenueNet',
                      potential_summands=[
                          'SalesRevenueGoodsNet',
                          'SalesRevenueServicesNet'],
                      optional_summands=[
                          'OtherSalesRevenueNet'
                      ]),
            CopyTagRule(original='SalesRevenueNet', target='Revenues'),

            SubtractFromRule(
                subtract_from_tag='RevenueFromContractWithCustomerIncludingAssessedTax',
                potential_subtract_tags=['ExciseAndSalesTaxes'],
                target_tag='RevenueFromContractWithCustomerExcludingAssessedTax'),

            CopyTagRule(original='RevenueFromContractWithCustomerExcludingAssessedTax',
                        target='Revenues'),
            CopyTagRule(original='RevenueFromContractWithCustomerIncludingAssessedTax',
                        target='Revenues'),
            CopyTagRule(original='RevenuesExcludingInterestAndDividends',
                        target='Revenues'),

            CopyTagRule(original='InterestAndDividendIncomeOperating', target='Revenues'),

            SumUpRule(sum_tag='RevenuesSum',
                      potential_summands=[
                          'RegulatedAndUnregulatedOperatingRevenue',
                          'HealthCareOrganizationPatientServiceRevenue',
                          'SalesRevenueGoodsGross',
                          'ContractsRevenue',
                          'RevenueOilAndGasServices',
                          'HealthCareOrganizationRevenue',
                          'RevenueMineralSales',
                          'SalesRevenueEnergyServices',
                          'RealEstateRevenueNet',
                          'InterestIncomeExpenseNet',
                          'NoninterestIncome',
                          'OperatingLeasesIncomeStatementLeaseRevenue',
                          'LicensesRevenue', 'RevenueFromRelatedParties',
                          'BrokerageCommissionsRevenue', 'RoyaltyRevenue', 'OilAndGasSalesRevenue',
                          'OilAndGasRevenue', 'OtherRealEstateRevenue',
                          'TechnologyServicesRevenue', 'ManagementFeesRevenue',
                          'ReimbursementRevenue',
                          'OperatingLeasesIncomeStatementMinimumLeaseRevenue',
                          'FoodAndBeverageRevenue', 'MaintenanceRevenue',
                          'LicenseAndServicesRevenue', 'FranchiseRevenue', 'SubscriptionRevenue',
                          'FinancialServicesRevenue',
                          'RevenueFromGrants',
                          'GasGatheringTransportationMarketingAndProcessingRevenue',
                          'OccupancyRevenue', 'NaturalGasProductionRevenue',
                          'SalesRevenueServicesGross', 'InvestmentBankingRevenue',
                          'AdvertisingRevenue', 'RevenueOtherFinancialServices',
                          'OilAndCondensateRevenue', 'RevenueFromLeasedAndOwnedHotels',
                          'RevenuesNetOfInterestExpense', 'RegulatedAndUnregulatedOperatingRevenue',
                          'UnregulatedOperatingRevenue', 'ElectricUtilityRevenue',
                          'CargoAndFreightRevenue', 'OtherHotelOperatingRevenue',
                          'CasinoRevenue', 'RefiningAndMarketingRevenue',
                          'PrincipalTransactionsRevenue', 'InterestRevenueExpenseNet',
                          'HomeBuildingRevenue', 'OtherRevenueExpenseFromRealEstateOperations',
                          'GasDomesticRegulatedRevenue', 'LicenseAndMaintenanceRevenue',
                          'RegulatedOperatingRevenue', 'AdmissionsRevenue', 'PassengerRevenue'
                      ],
                      optional_summands=[
                          'OtherSalesRevenueNet'
                      ]),

            CopyTagRule(original='RevenuesSum', target='Revenues'),
        ]
    )

    is_netincome_rg = RuleGroup(
        prefix="netincome",
        rules=[
            CopyTagRule(original='NetIncomeLossAvailableToCommonStockholdersBasic',
                        target='NetIncomeLoss'),
            CopyTagRule(original='NetIncomeLossAllocatedToLimitedPartners', target='NetIncomeLoss'),
            CopyTagRule(original='ProfitLoss', target='NetIncomeLoss'),
            CopyTagRule(original='OtherComprehensiveIncomeLossNetOfTax', target='NetIncomeLoss'),
            CopyTagRule(original='ComprehensiveIncomeNetOfTax', target='NetIncomeLoss'),
            CopyTagRule(
                original='IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest',
                target='NetIncomeLoss'),
        ]
    )

    main_rule_tree = RuleGroup(prefix="IS",
                               rules=[
                                   is_revenue_rg,
                                   is_netincome_rg
                               ])

    preprocess_rule_tree = RuleGroup(prefix="IS_PRE",
                                     rules=[
                                     ])

    post_rule_tree = RuleGroup(prefix="IS_POST",
                               rules=[
                               ])

    validation_rules: List[ValidationRule] = [
    ]

    # these are the columns that finally are returned after the standardization
    final_tags: List[str] = ['Revenues',
                             'CostOfRevenue',
                             'GrossProfit',
                             'OperatingIncomeLoss',
                             'IncomeLossBeforeIncomeTaxExpenseBenefit',
                             'IncomeTaxExpenseBenefit',
                             'NetIncomeLoss'
                             ]

    # used to evaluate if a report is the main balancesheet report
    # inside a report, there can be several tables (different report nr)
    # which stmt value is BS.
    # however, we might be only interested in the "major" BS report. Usually this is the
    # one which has the least nan in the following columns
    main_statement_tags = ['Revenues',
                           'SalesRevenueNet',
                           'SalesRevenueGoodsNet',
                           'SalesRevenueServicesNet',
                           'CostOfRevenue',
                           'CostOfGoodsAndServicesSold',
                           'CostOfGoodsSold',
                           'CostOfServices',
                           'GrossProfit',
                           'OperatingExpenses',
                           'OperatingIncomeLoss',
                           'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
                           'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
                           'IncomeLossBeforeIncomeTaxExpenseBenefit',
                           'IncomeTaxExpenseBenefit',
                           'NetIncomeLoss',
                           'ProfitLoss'
                           ]

    def __init__(self, filter_for_main_statement: bool = True, iterations: int = 3):
        super().__init__(
            prepivot_rule_tree=self.prepivot_rule_tree,
            pre_rule_tree=self.preprocess_rule_tree,
            main_rule_tree=self.main_rule_tree,
            post_rule_tree=self.post_rule_tree,
            validation_rules=self.validation_rules,
            final_tags=self.final_tags,
            main_iterations=iterations,
            filter_for_main_statement=filter_for_main_statement,
            main_statement_tags=self.main_statement_tags)
